{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE PatternGuards              #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# LANGUAGE MonoLocalBinds             #-}

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
 , Rotation
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
  , Message
  , link
  , unmonitor
  , unwrapMessage
  , getSelfPid
  )
import Control.Distributed.Process.Extras
  ( spawnMonitorLocal
  , awaitExit
  , Shutdown(..)
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
 , (^.)
 )
import Data.Binary
import Data.Foldable (forM_)
import Data.Typeable (Typeable)
import Data.Hashable (Hashable)
import Data.HashSet (HashSet)
import qualified Data.HashSet as HashSet (empty, insert, delete, member)
import Data.Rank1Typeable (typeOf)
import GHC.Generics

-- | The size of a worker pool
type PoolSize = Integer

-- | The rotation policy applied to a worker pool
type Rotation = RotationPolicy WPState Worker

-- | A handle to a worker process
newtype Worker = Worker { unWorker :: (ProcessId, MonitorRef) }
  deriving (Typeable, Generic, Ord, Show)

instance Eq Worker where
  (Worker (p, m)) == (Worker (p', m')) = p == p' && m == m'

instance Binary Worker where
instance NFData Worker where
instance Hashable Worker where

instance Linkable Worker where
  linkTo = link . fst . unWorker

instance Resolvable Worker where
  resolve = return . Just . fst . unWorker

instance Routable Worker where
  sendTo = sendTo . fst . unWorker
  unsafeSendTo = unsafeSendTo . fst . unWorker

data WPState = WPState { sizeLimit :: PoolSize
                       , monitors  :: HashSet MonitorRef
                       }

szLimit :: Accessor WPState PoolSize
szLimit = accessor sizeLimit (\sz' wps -> wps { sizeLimit = sz' })

workerRefs :: Accessor WPState (HashSet MonitorRef)
workerRefs = accessor monitors (\ms' wps -> wps { monitors = ms' })

-- | Create a 'Resource' specification for a given @Process ()@.
worker :: Process () -> Resource Worker
worker w =
  Resource {
      create   = getSelfPid >>= \p -> Worker <$> spawnMonitorLocal (link p >> w)
    , destroy  = \(Worker (p, r))  -> unmonitor r >> exit p Shutdown >> awaitExit p
    , checkRef = \m (Worker (_, r)) ->
         handleMessageIf m (\(ProcessMonitorNotification r' _ _) -> r == r')
                           (\_ -> return Dead)
    , accept   = \t r -> unsafeSendChan (ticketChan t) r
    }

-- | Run a worker pool with the supplied configuration
runWorkerPool :: Process ()
              -> PoolSize
              -> InitPolicy
              -> Rotation
              -> ReclamationStrategy
              -> Process ()
runWorkerPool rt sz ip rp rs = runPool (worker rt) poolDef ip rp rs (initState sz)

initState :: PoolSize -> WPState
initState sz = WPState sz HashSet.empty

poolDef :: PoolBackend WPState Worker
poolDef = PoolBackend { acquire  = apiAcquire
                      , release  = releasePooledResource
                      , dispose  = apiDispose
                      , setup    = apiSetup
                      , teardown = apiTeardown
                      , infoCall = apiInfoCall
                      , getStats = apiGetStats
                      }

getLimit :: Pool WPState Worker Integer
getLimit = sizeLimit <$> getState

apiAcquire  :: Pool WPState Worker (Take Worker)
apiAcquire = do
  pol <- getInitPolicy
  sz  <- getLimit
  (a, b) <- resourceQueueLen
  maybeAcquire pol (toInteger a) (toInteger b) sz

  where

    maybeAcquire pol' free taken limit'
      | taken >= limit'  = return Block
      | free > 0         = doAcquire
      | OnDemand <- pol'
      , free == 0        = doCreate
      | otherwise        = doAcquire

    doAcquire = maybe Block Take <$> acquirePooledResource
    doCreate =
      getResourceType >>= lift . create >>= stashResource >> doAcquire

apiDispose :: Worker -> Pool WPState Worker ()
apiDispose r@Worker{..} = do
  rType <- getResourceType
  lift $ destroy rType r
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
      when (exceedsLimit cnt $ st ^. szLimit) $
        do rType <- getResourceType
           res <- lift $ create rType
           stashResource res
           startResources (cnt + 1)

    exceedsLimit :: PoolSize -> PoolSize -> Bool
    exceedsLimit = (<)

apiTeardown :: ExitReason -> Pool WPState Worker ()
apiTeardown = const $ foldResources (const apiDispose) ()

apiInfoCall :: Message -> Pool WPState Worker ()
apiInfoCall msg = do
  -- If this is a monitor signal and pertains to one of our resources,
  -- we need to permanently remove it so its ref doesn't leak to
  -- some unfortunate consumer (who would naturally expect the pid to be valid).
  mSig <- lift $ checkMonitor msg
  forM_ mSig checkRefs

  where
    checkMonitor :: Message -> Process (Maybe ProcessMonitorNotification)
    checkMonitor = unwrapMessage

    checkRefs :: ProcessMonitorNotification -> Pool WPState Worker ()
    checkRefs (ProcessMonitorNotification r p _) = do
      mRefs <- (^. workerRefs) <$> getState
      when (HashSet.member r mRefs) $ do
        modifyState (workerRefs ^: HashSet.delete r)
        deletePooledResource $ Worker (p, r)

apiGetStats :: Pool WPState Worker [PoolStatsInfo]
apiGetStats = do
  st <- getState
  return [ PoolStatsInfo     "pool.backend.name"             "WorkerPool"
         , PoolStatsInfo     "pool.backend.impl"             "Control.Distributed.Process.Task.Pool.WorkerPool"
         , PoolStatsInfo     "pool.backend.resource.name"    "Worker"
         , PoolStatsTypeInfo "pool.backend.resource.type"    $ typeOf (undefined :: Process ())
         , PoolStatsCounter  "pool.backend.config.sizeLimit" $ st ^. szLimit
--         , PoolStatsCounter "available" a
--         , PoolStatsCounter "busy" b
         ]

stashResource :: Worker -> Pool WPState Worker ()
stashResource res@(Worker (_, ref)) = do
  modifyState (workerRefs ^: HashSet.insert ref)
  addPooledResource res
