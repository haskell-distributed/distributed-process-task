{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE PatternGuards              #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE UndecidableInstances       #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Task.Pool.WorkerPool
-- Copyright   :  (c) Tim Watson 2013
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- [Process Pool (Backend)]
--
-- This module implements a pool of worker processes, implemented with the
-- 'BackingPool' API from "Control.Distributed.Process.Task.Pool'.
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Task.Pool.WorkerPool
 ( worker
 , runWorkerPool
 , Worker
 , PoolSize
 ) where

import Control.DeepSeq (NFData)
import Control.Distributed.Process
  ( Process
  , MonitorRef
  , ProcessMonitorNotification(..)
  , ProcessId
  , handleMessageIf
  , unsafeSendChan
  , exit
  , match
  , Message
  , link
  , monitor
  , unmonitor
  , unwrapMessage
  , getSelfPid
  , liftIO
  )
import Control.Distributed.Process.Extras
  ( spawnMonitorLocal
  , awaitExit
  , Shutdown(..)
  , NFSerializable
  , ExitReason(..)
  , Linkable(..)
  , Routable(..)
  , Resolvable(..)
  )
import Control.Distributed.Process.Task.Pool.Backend
import Control.Monad (when)
import Data.Accessor
 ( Accessor
 , accessor
 , (^:)
 , (^=)
 , (.>)
 , (^.)
 )
import Data.Binary
import Data.Typeable (Typeable)
import Data.Hashable (Hashable)
import Data.HashSet (HashSet)
import qualified Data.HashSet as HashSet (empty, insert, delete, member)
import Data.Rank1Typeable (typeOf)
import GHC.Generics

type PoolSize = Integer

newtype Worker = Worker { unWorker :: (ProcessId, MonitorRef) }
  deriving (Typeable, Generic, Ord, Show)

instance Eq Worker where
  (Worker (p, m)) == (Worker (p', m')) = p == p && m == m'

instance Binary Worker where
instance NFData Worker where
instance NFSerializable Worker
instance Hashable Worker where

instance Linkable Worker where
  linkTo = link . fst . unWorker

instance Resolvable Worker where
  resolve = return . Just . fst . unWorker

instance Routable Worker where
  sendTo = sendTo . fst . unWorker
  unsafeSendTo = unsafeSendTo . fst . unWorker

worker :: Process () -> Resource Worker
worker w =
  Resource {
      create   = getSelfPid >>= \p -> spawnMonitorLocal (link p >> w) >>= return . Worker
    , destroy  = \(Worker (p, r))  -> unmonitor r >> exit p Shutdown >> awaitExit p
    , checkRef = (\m (Worker (_, r)) -> do
         handleMessageIf m (\(ProcessMonitorNotification r' _ _) -> r == r')
                           (\_ -> return Dead))
    , accept   = \t r -> unsafeSendChan (ticketChan t) r
    }

data WPState = WPState { sizeLimit :: PoolSize
                       , monitors  :: HashSet MonitorRef
                       }

szLimit :: Accessor WPState PoolSize
szLimit = accessor sizeLimit (\sz' wps -> wps { sizeLimit = sz' })

workerRefs :: Accessor WPState (HashSet MonitorRef)
workerRefs = accessor monitors (\ms' wps -> wps { monitors = ms' })

runWorkerPool :: Process ()
              -> PoolSize
              -> InitPolicy
              -> RotationPolicy WPState Worker
              -> ReclamationStrategy
              -> Process ()
runWorkerPool rt sz ip rp rs = runPool (worker rt) poolDef ip rp rs (initState sz)

initState :: PoolSize -> WPState
initState sz = WPState sz HashSet.empty

poolDef :: PoolBackend WPState Worker
poolDef = PoolBackend { acquire  = apiAcquire
                      , release  = apiRelease
                      , dispose  = apiDispose
                      , setup    = apiSetup
                      , teardown = apiTeardown
                      , infoCall = apiInfoCall
                      , getStats = apiGetStats
                      }

getLimit :: Pool WPState Worker Integer
getLimit = getState >>= return . sizeLimit

apiAcquire  :: Pool WPState Worker (Take Worker)
apiAcquire = do
  pol <- getInitPolicy
  sz  <- getLimit
  (a, b) <- resourceQueueLen
  lift $ liftIO $ putStrLn $ "resource q = " ++ (show ((a, b), sz))

  maybeAcquire pol (toInteger a) (toInteger b) sz

  where

    maybeAcquire pol' free taken limit'
      | taken >= limit'  = return Block
      | free > 0         = doAcquire
      | OnDemand <- pol'
      , free == 0        = doCreate
      | otherwise        = doAcquire

    doAcquire = return . maybe Block Take =<< acquirePooledResource
    doCreate =
      getResourceType >>= lift . create >>= stashResource >> doAcquire

apiRelease :: Worker -> Pool WPState Worker ()
apiRelease res = do
  releasePooledResource res

apiDispose :: Worker -> Pool WPState Worker ()
apiDispose r@Worker{..} = do
  rType <- getResourceType
  res <- lift $ destroy rType r
  deletePooledResource r
  modifyState (workerRefs ^: HashSet.delete (snd unWorker))

apiSetup :: Pool WPState Worker ()
apiSetup = do
  pol <- getInitPolicy
  case pol of
    OnDemand -> return ()
    OnInit   -> startResources 0
  where
    startResources :: PoolSize -> Pool WPState Worker ()
    startResources cnt = do
      st <- getState :: Pool WPState Worker WPState
      if cnt < (sizeLimit st)
         then do rType <- getResourceType
                 res <- lift $ create rType
                 stashResource res
                 startResources (cnt + 1)
         else return ()

apiTeardown :: ExitReason -> Pool WPState Worker ()
apiTeardown = const $ foldResources (const apiDispose) ()

apiInfoCall :: Message -> Pool WPState Worker ()
apiInfoCall msg = do
  -- If this is a monitor signal and pertains to one of our resources,
  -- we need to permanently remove it so its ref doesn't leak to
  -- some unfortunate consumer (who would naturally expect the pid to be valid).
  mSig <- lift $ checkMonitor msg
  case mSig of
    Nothing   -> return ()
    Just mRef -> checkRefs mRef

  where
    checkMonitor :: Message -> Process (Maybe ProcessMonitorNotification)
    checkMonitor = unwrapMessage

    checkRefs :: ProcessMonitorNotification -> Pool WPState Worker ()
    checkRefs (ProcessMonitorNotification r p _) = do
      mRefs <- getState >>= return . (^. workerRefs)
      when (HashSet.member r mRefs) $ do
        lift $ liftIO $ putStrLn "deleting worker"
        modifyState (workerRefs ^: HashSet.delete r)
        deletePooledResource $ Worker (p, r)

apiGetStats :: Pool WPState Worker [PoolStatsInfo]
apiGetStats = do
  st <- getState
  return [ PoolStatsInfo     "pool.backend.name"            "WorkerPool"
         , PoolStatsInfo     "pool.backend.impl"            "Control.Distributed.Process.Task.Pool.WorkerPool"
         , PoolStatsInfo     "pool.backend.resource.name"   "Worker"
         , PoolStatsTypeInfo "pool.backend.resource.type"   $ typeOf (undefined :: Process ())
         , PoolStatsCounter  "pool.backend.config.sizeLimi" $ st ^. szLimit
--         , PoolStatsCounter "available" a
--         , PoolStatsCounter "busy" b
         ]

stashResource :: Worker -> Pool WPState Worker ()
stashResource res@(Worker (_, ref)) = do
  modifyState (workerRefs ^: HashSet.insert ref)
  addPooledResource res
