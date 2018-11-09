{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE PatternGuards              #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE UndecidableInstances       #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Task.Pool.Backend
-- Copyright   :  (c) Tim Watson 2017
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- =Pool (Backend) API
--
-- This module implements the /backing pool/ API for "Control.Distributed.Process.Task.Pool".
--
-- A pool backend implements the worker handling, while the generic pool process
-- handles the client-server protocol. This means that pool implementors can leave
-- issues such as handling client disconnection or termination, tracking pending
-- client requests (where resources are queued up due to high demand), and the
-- application of initialisation, and concentrate on things such as balancing
-- access to resources, and using feedback mechanisms to alter system behaviour
-- at runtime.
--
-- The backend api is defined in functions supplied as fields of the 'PoolBackend'
-- record type.
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Task.Pool.Backend
 ( -- Running a backend implementation
   runPool
   -- State Management & Views
 , getState
 , putState
 , modifyState
 , resourceQueueLen
 , getInitPolicy
 , getRotationPolicy
 , getResourceType
   -- Resource specific management APIs
 , foldResources
 , addPooledResource
 , releasePooledResource
 , acquirePooledResource
 , removePooledResource
 , retirePooledResource
 , deletePooledResource
   -- Pool Monad & Common Types
 , lift
 , Pool
 , Referenced
 , Take(..)
 , Ticket(..)
 , Status(..)
 , InitPolicy(..)
 , RotationPolicy(..)
 , ReclamationStrategy(..)
 , Resource(..)
 , ResourceQueue(..)
 , PoolState
 , PoolStats(..)
 , PoolStatsInfo(..)
 , PoolBackend(..)
 ) where

import Control.Distributed.Process
  ( Process
  )
import Control.Distributed.Process.Task.Pool.Internal.Process (runPool)
import Control.Distributed.Process.Task.Pool.Internal.Types
import qualified Control.Monad.State as ST
  ( get
  , lift
  , modify
  , put
  )
import Data.Accessor
  ( (^:)
  , (^=)
  , (.>)
  , (^.)
  )
import Data.Foldable (foldlM)
import Data.Sequence
  ( Seq
  , ViewR(..)
  , ViewL(..)
  , (<|)
  , (|>)
  , viewr
  , viewl
  )
import qualified Data.Sequence as Seq (length, filter)
import qualified Data.Set as Set (size, insert, delete)

getState :: forall s r. Pool s r s
getState = ST.get >>= return . internalState

putState :: forall s r. s -> Pool s r ()
putState st = modifyPoolState $ \s -> s { internalState = st }

modifyState :: forall s r. (s -> s) -> Pool s r ()
modifyState fn =
  modifyPoolState $ \s -> s { internalState = (fn $ internalState s) }

-- TODO MonadTrans instance? lift :: (Monad m) => m a -> t m a

-- | Lift an action in the Process monad.
lift :: forall s r a . Process a -> Pool s r a
lift p = Pool $ ST.lift p

foldResources :: forall s r a. (Referenced r)
              => (a -> r -> Pool s r a)
              -> a
              -> Pool s r a
foldResources f a = do
  st <- getPoolState
  let rq = (st ^. resourceQueue)
  a' <- foldlM f a  (_available rq)
  b' <- foldlM f a' (_busy rq)
  return b'

resourceQueueLen :: forall s r . (Referenced r) => Pool s r (Int, Int)
resourceQueueLen = do
  st <- getPoolState
  return $ lengths (st ^. resourceQueue)
  where
    lengths ResourceQueue{..} = (Seq.length _available, Set.size _busy)

addPooledResource :: forall s r. (Referenced r) => r -> Pool s r ()
addPooledResource = releasePooledResource

releasePooledResource :: forall s r . (Referenced r) => r -> Pool s r ()
releasePooledResource = modifyPoolState . doReleasePooledResource

acquirePooledResource :: forall s r. (Referenced r) => Pool s r (Maybe r)
acquirePooledResource = do
  st <- getPoolState
  let ac' = doAcquirePooledResource st
  case ac' of
    Nothing       -> return Nothing
    Just (r, st') -> do putPoolState st'
                        return $ Just r

removePooledResource :: forall s r. (Referenced r) => r -> Pool s r ()
removePooledResource r = do
    st <- getPoolState
    case clear st of
      Nothing -> modifyPoolState (resourceQueue .> available ^: Seq.filter (/= r))
      Just q  -> modifyPoolState (resourceQueue .> available ^= q)
  where
    clear :: PoolState s r -> Maybe (Seq r)
    clear ps =
      case remove (Right . viewr) ps of
        Nothing    -> remove (Left . viewl) ps
        s@(Just _) -> s

    remove :: (Seq r -> Either (ViewL r) (ViewR r))
           -> PoolState s r
           -> Maybe (Seq r)
    remove f st =
      case f (st ^. resourceQueue .> available) of
        Right EmptyR             -> Nothing
        Right (q' :> a) | a == r -> Just q'
        Left  EmptyL             -> Nothing
        Left  (a :< q') | a == r -> Just q'
        _                        -> Nothing

retirePooledResource :: forall s r . (Referenced r) => r -> Pool s r ()
retirePooledResource r = modifyPoolState (resourceQueue .> busy ^: Set.delete r)

deletePooledResource :: forall s r . (Referenced r) => r -> Pool s r ()
deletePooledResource r = removePooledResource r >> retirePooledResource r

getInitPolicy :: forall s r . Pool s r InitPolicy
getInitPolicy = ST.get >>= return . initPolicy

getRotationPolicy :: forall s r . Pool s r (RotationPolicy s r)
getRotationPolicy = ST.get >>= return . rotationPolicy

getResourceType :: forall s r . Pool s r (Resource r)
getResourceType = do
  st <- getPoolState
  return (resourceType st)

getPoolState :: forall s r. Pool s r (PoolState s r)
getPoolState = ST.get

putPoolState :: forall s r. PoolState s r -> Pool s r ()
putPoolState s = ST.put $ s { isDirty = True }

modifyPoolState :: forall s r. (PoolState s r -> PoolState s r) -> Pool s r ()
modifyPoolState f = ST.modify (\s -> f $ s { isDirty = True})

doAcquirePooledResource :: forall s r . (Referenced r)
                        => PoolState s r -> Maybe (r, PoolState s r)
doAcquirePooledResource st =
  case (viewr avail) of
    EmptyR    -> Nothing
    (q' :> a) -> Just (a, ( (resourceQueue .> available ^= q')
                          . (resourceQueue .> busy ^: Set.insert a) $ st ))
  where
    avail = st ^. resourceQueue .> available

doReleasePooledResource :: forall s r . (Referenced r)
                        => r -> PoolState s r -> PoolState s r
doReleasePooledResource r st
  | (Custom fn) <- rp = fn st r
  | otherwise {- LRU or MRU -}    =
    let avail = case rp of
                  LRU -> (r <|)
                  MRU -> (|> r)
                  _   -> error "Pool.InvalidRotationPolicy"
    in ( (resourceQueue .> available ^: avail)
       . (resourceQueue .> busy ^: Set.delete r) $ st )
  where
    rp = rotationPolicy st
