{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE PatternGuards              #-}
{-# LANGUAGE RecordWildCards            #-}
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
   transaction
 , acquireResource
 , releaseResource
 , checkOut
 , checkIn
 , transfer
 , start
   -- Defining A Pool Backend
 , module Backend
   -- Types
 , ResourcePool
 , InitPolicy(..)
 , RotationPolicy(..)
 , ReclamationStrategy(..)
 ) where

import Control.Distributed.Process
  ( Process
  , ProcessId
  , receiveChan
  , spawnLocal
  , getSelfPid
  )
import Control.Distributed.Process.ManagedProcess.Client
  ( callChan
  , cast
  )
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
 , PoolStats(..)
 , PoolStatsInfo(..)
 , Resource(..)
 , PoolBackend(..)
 , PoolState
 , Pool
 )
-- import Control.Distributed.Process.Supervisor (SupervisorPid)
import Control.Monad.Catch (finally)

--------------------------------------------------------------------------------
-- Client Facing API                                                          --
--------------------------------------------------------------------------------

transaction :: forall a r. (Referenced r)
        => ResourcePool r
        -> (r -> Process a)
        -> Process a
transaction pool proc = do
  acquireResource pool >>= \r -> proc r `finally` releaseResource pool r

checkOut :: forall r. (Referenced r) => ResourcePool r -> Process r
checkOut = acquireResource

acquireResource :: forall r. (Referenced r)
                => ResourcePool r
                -> Process r
acquireResource pool =
  getSelfPid >>= callChan pool . AcquireResource >>= receiveChan

checkIn :: forall r. (Referenced r) => ResourcePool r -> r -> Process ()
checkIn = releaseResource

releaseResource :: forall r. (Referenced r)
                => ResourcePool r
                -> r
                -> Process ()
releaseResource pool res = getSelfPid >>= cast pool . ReleaseResource res

transfer :: forall r. (Referenced r)
         => ResourcePool r
         -> r
         -> ProcessId
         -> Process ()
transfer = undefined

--------------------------------------------------------------------------------
-- Starting/Running a Resource Pool                                           --
--------------------------------------------------------------------------------

start :: forall r . (Referenced r)
      => Process ()
      -> Process (ResourcePool r)
start p = spawnLocal p >>= return . ResourcePool
