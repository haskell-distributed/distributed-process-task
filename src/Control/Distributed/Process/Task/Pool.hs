{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE UndecidableInstances       #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Task.Pool
-- Copyright   :  (c) Tim Watson 2013
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- A pool of resources. The resources are created when the pool starts, by
-- evaluating a /generator/ expression. Resources are acquired and released
-- by client interaction with the pool process. Once acquired, a resource
-- is /locked/ by the pool and will not be offered to future clients until
-- they're released.
--
-- If a client (process) crashes whilst the resource is locked, the pool will
-- either release the resource automatically, destroy it, or leave it
-- permanently locked - depending on the 'ReclamationStrategy' chosen during
-- pool initialisation. The pool also monitors resources - for this reason, only
-- resource types that implement the 'Resource' typeclass can be used - and
-- if they crash, will restart them on demand.
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Task.Pool
 ( -- Client Facing API
   acquireResource
 , releaseResource
 , transaction
 , checkOut
 , checkIn
 , withResource
 , transfer
 , transferTo
 , poolStats
 , startPool
   -- Defining A Pool Backend
 , module Backend
   -- Types
 , ResourcePool
 , InitPolicy(..)
 , RotationPolicy(..)
 , ReclamationStrategy(..)
 , TransferResponse(..)
 ) where

import Control.Distributed.Process
  ( Process
  , ProcessId
  , receiveChan
  , spawnLocal
  , getSelfPid
  )
import Control.Distributed.Process.ManagedProcess.UnsafeClient
  ( callChan
  , cast
  , call
  )
import qualified Control.Distributed.Process.ManagedProcess.Client as SafeClient (call)
import qualified Control.Distributed.Process.Task.Pool.WorkerPool as WorkerPool
import qualified Control.Distributed.Process.Task.Pool.Backend as Backend
import Control.Distributed.Process.Task.Pool.Internal.Types
 ( Referenced
 , InitPolicy(..)
 , RotationPolicy(..)
 , ResourcePool(..)
 , ReclamationStrategy(..)
 , AcquireResource(..)
 , ReleaseResource(..)
 , TransferRequest(..)
 , TransferResponse(..)
 , StatsReq(..)
 , PoolStats(..)
 , PoolStatsInfo(..)
 , Resource(..)
 , PoolBackend(..)
 , PoolState
 , Pool
 )
-- import Control.Distributed.Process.Supervisor (SupervisorPid)
import Control.Monad.Catch (finally, mask)

--------------------------------------------------------------------------------
-- Client Facing API                                                          --
--------------------------------------------------------------------------------

-- | Acquire a resource of type /r/ from the pool.
--
-- > acquireResource poolHandle >>= doSomethingWithResource
--
acquireResource :: forall r. (Referenced r)
                => ResourcePool r
                -> Process r
acquireResource pool =
  getSelfPid >>= callChan pool . AcquireResource >>= receiveChan

-- | Release a resource of type /r/, previously acquired from the pool.
--
-- > acquireResource poolHandle >>= \r -> doSomethingWithResource >> releaseResource r
--
releaseResource :: forall r. (Referenced r)
                => ResourcePool r
                -> r
                -> Process ()
releaseResource pool res = getSelfPid >>= cast pool . ReleaseResource res

-- | Acquire a resource of type /r/ from the pool, operate on it, and released
-- the resource once we're finished.
--
-- Asynchronous exceptions are masked during acquisition and release, and
-- restored only during the execution of the user supplied operation.
--
-- > acquireResource poolHandle >>= doSomethingWithResource
--
transaction :: forall a r. (Referenced r)
            => ResourcePool r
            -> (r -> Process a)
            -> Process a
transaction pool proc =
  mask $ \restore -> do
    r <- acquireResource pool
    finally (restore $ proc r)
            (releaseResource pool r)

-- | Synonym for 'acquireResource'
checkOut :: forall r. (Referenced r) => ResourcePool r -> Process r
checkOut = acquireResource

-- | Synonym for 'releaseResource'
checkIn :: forall r. (Referenced r) => ResourcePool r -> r -> Process ()
checkIn = releaseResource

-- | Synonym for 'transaction'
withResource :: forall a r. (Referenced r)
             => ResourcePool r
             -> (r -> Process a)
             -> Process a
withResource = transaction

-- | Transfer a resource owned by this (calling) process, to the suppled 'ProcessId'
transferTo :: forall r. (Referenced r)
           => ResourcePool r
           -> r
           -> ProcessId
           -> Process TransferResponse
transferTo pool res pid = getSelfPid >>= \us -> transfer pool us res pid

-- | Transfer a resource owned by the first 'ProcessId', over to the second 'ProcessId'
transfer :: forall r . (Referenced r)
         => ResourcePool r
         -> ProcessId
         -> r
         -> ProcessId
         -> Process TransferResponse
transfer pool old res new = call pool $ TransferRequest res new old

-- | Retrieve statistics for the pool refered to by the supplied handle.
poolStats :: forall r . Referenced r
             => ResourcePool r
             -> Process PoolStats
poolStats pool = SafeClient.call pool StatsReq

--------------------------------------------------------------------------------
-- Starting/Running a Resource Pool                                           --
--------------------------------------------------------------------------------

-- | Spawns the pool process given as @Process ()@ and wraps it in a 'ResourcePool' handle.
startPool :: forall r . (Referenced r)
          => Process ()
          -> Process (ResourcePool r)
startPool p = ResourcePool <$> spawnLocal p
