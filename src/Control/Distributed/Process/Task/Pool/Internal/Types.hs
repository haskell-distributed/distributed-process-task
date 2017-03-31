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
-- Module      :  Control.Distributed.Process.Task.Pool.Internal.Types
-- Copyright   :  (c) Tim Watson 2017
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- = WARNING
--
-- This module is considered __internal__.
--
-- The Package Versioning Policy __does not apply__.
--
-- This contents of this module may change __in any way whatsoever__
-- and __without any warning__ between minor versions of this package.
--
-- Authors importing this module are expected to track development
-- closely.
--
-- [Process Pool (Types)]
--
-- This module contains types common to all pool implementations.
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Task.Pool.Internal.Types
 ( -- Client Handles & Command Messages
   ResourcePool(..)
 , AcquireResource(..)
 , ReleaseResource(..)
 , StatsReq(..)
   -- Statistics
 , PoolStats(..)
 , PoolStatsInfo(..)
   -- Policy, Strategy, and Internal
 , ReclamationStrategy(..)
 , Status(..)
 , InitPolicy(..)
 , RotationPolicy(..)
 , Take(..)
 , Ticket(..)
 , baseErrorMessage
   -- Resource Types, Pool Backends and the Pool monad.
 , Referenced
 , Resource(..)
 , ResourceQueue(..)
 , PoolBackend(..)
 , PoolState(..)
 , initialPoolState
 , Pool(..)
   -- Lenses
 , resourceQueue
 , queueRotationPolicy
 , available
 , busy
 ) where

import Control.DeepSeq (NFData)
import Control.Distributed.Process
  ( Process
  , ProcessId
  , Message
  , SendPort
  , link
  )
import Control.Distributed.Process.Extras.Internal.Types
  ( Resolvable(..)
  , Routable(..)
  , Addressable
  , Linkable(..)
  , NFSerializable
  , ExitReason(..)
  )
-- import Control.Distributed.Process.Supervisor (SupervisorPid)
import Control.Distributed.Process.Serializable
import Control.Monad.IO.Class (MonadIO)
import qualified Control.Monad.State as ST
  ( StateT
  , MonadState
  )

import Data.Accessor (Accessor, accessor)
import Data.Binary
import Data.Hashable
import Data.Sequence (Seq)
import qualified Data.Sequence as Seq (empty)
import Data.Set (Set)
import qualified Data.Set as Set (empty)
import Data.Rank1Typeable (TypeRep)
import Data.Typeable (Typeable)
import GHC.Generics

--------------------------------------------------------------------------------
-- Client Handles & Command Messages                                          --
--------------------------------------------------------------------------------

data ResourcePool r = ResourcePool { poolAddr :: !ProcessId }
  deriving (Typeable, Generic, Eq, Show)
instance (Serializable r) => Binary (ResourcePool r) where
instance (NFData r) => NFData (ResourcePool r) where
instance (Serializable r, NFData r) => NFSerializable (ResourcePool r) where

instance Linkable (ResourcePool r) where
  linkTo = link . poolAddr

instance Resolvable (ResourcePool r) where
  resolve = return . Just . poolAddr

instance Routable (ResourcePool r) where
  sendTo = sendTo . poolAddr
  unsafeSendTo = unsafeSendTo . poolAddr

instance Addressable (ResourcePool r)

class (Serializable r, Hashable r, Eq r, Ord r, Show r) => Referenced r
instance (Serializable r, Hashable r, Eq r, Ord r, Show r) => Referenced r

data AcquireResource = AcquireResource ProcessId
  deriving (Typeable, Generic, Eq, Show)
instance Binary AcquireResource where
instance NFData AcquireResource where
instance NFSerializable AcquireResource

data ReleaseResource r = ReleaseResource r ProcessId
  deriving (Typeable, Generic)
instance (Serializable r) => Binary (ReleaseResource r) where
instance (NFData r) => NFData (ReleaseResource r) where
instance (Serializable r, NFData r) => NFSerializable (ReleaseResource r)

data StatsReq = StatsReq deriving (Typeable, Generic)
instance Binary StatsReq where
instance NFData StatsReq where
instance NFSerializable StatsReq

data Ticket r = Ticket { ticketOwner :: ProcessId
                       , ticketChan  :: SendPort r
                       } deriving (Typeable)

baseErrorMessage :: String
baseErrorMessage = "Control.Distributed.Process.Task.Pool"

--------------------------------------------------------------------------------
-- Statistics                                                                 --
--------------------------------------------------------------------------------

data PoolStatsInfo = PoolStatsInfo     String String  |
                     PoolStatsTypeInfo String TypeRep |
                     PoolStatsData     String TypeRep Message |
                     PoolStatsCounter  String Integer |
                     PoolStatsFloat    String Double
 deriving (Typeable, Generic)
instance Binary PoolStatsInfo where

data PoolStats = PoolStats { totalResources    :: Integer
                           , activeResources   :: Int
                           , inactiveResources :: Int
                           , activeClients     :: Int
                           , pendingClients    :: Int
                           , backendStats      :: [PoolStatsInfo]
                           } deriving (Typeable, Generic)
instance Binary PoolStats where

--------------------------------------------------------------------------------
-- Resource Types, Pool Backends and the @Pool@ monad.                        --
--------------------------------------------------------------------------------

data ReclamationStrategy = Release | Destroy | PermLock deriving (Eq, Show)

data Status = Alive | Dead | Unknown

data InitPolicy = OnDemand | OnInit deriving (Typeable, Eq, Show)

-- We use 'RotationPolicy' to determine whether released resources are applied
-- to the head or tail of the ResourceQueue. We always dequeue the 'head'
-- element (i.e., the left side of the sequence).

data RotationPolicy s r = LRU
                        | MRU
                        | Custom (PoolState s r -> r -> PoolState s r)

data Take r = Block | Take r

instance (Eq r) => Eq (Take r) where
  Block    == Block    = True
  Block    == _        = False
  _        == Block    = False
  (Take a) == (Take b) = a == b

instance Show r => Show (Take r) where
  show Block    = "Block"
  show (Take r) = "Take[" ++ (show r) ++ "]"

data Resource r =
  Resource { create   :: Process r
           , destroy  :: r -> Process ()
           , checkRef :: Message -> r -> Process (Maybe Status)
           , accept   :: (Ticket r) -> r -> Process ()
           }

data ResourceQueue r = ResourceQueue { _available :: Seq r
                                     , _busy      :: Set r
                                     } deriving (Typeable)

data PoolBackend s r =
  PoolBackend { acquire  :: Pool s r (Take r)
              , release  :: r -> Pool s r ()
              , dispose  :: r -> Pool s r ()
              , setup    :: Pool s r ()
              , teardown :: ExitReason -> Pool s r ()
              , infoCall :: Message -> Pool s r ()
              , getStats :: Pool s r [PoolStatsInfo]
              }

data PoolState s r = PoolState { queue          :: ResourceQueue r
                               , initPolicy     :: InitPolicy
                               , rotationPolicy :: RotationPolicy s r
                               , internalState  :: s
                               , resourceType   :: Resource r
                               , isDirty        :: Bool
                               } deriving (Typeable)

initialPoolState :: forall s r. (Referenced r)
                 => Resource r
                 -> InitPolicy
                 -> RotationPolicy s r
                 -> s
                 -> PoolState s r
initialPoolState rt ip rp st =
  PoolState { queue          = ResourceQueue Seq.empty Set.empty
            , initPolicy     = ip
            , rotationPolicy = rp
            , internalState  = st
            , resourceType   = rt
            , isDirty        = False
            }

newtype Pool s r a = Pool { unPool :: ST.StateT (PoolState s r) Process a }
  deriving ( Functor
           , Monad
           , MonadIO
           , Typeable
           , Applicative
           , ST.MonadState (PoolState s r)
           )

-- PoolState and ResourceQueue lenses

resourceQueue :: forall s r. Accessor (PoolState s r) (ResourceQueue r)
resourceQueue = accessor queue (\q' st -> st { queue = q' })

queueRotationPolicy :: forall s r . Accessor (PoolState s r) (RotationPolicy s r)
queueRotationPolicy = accessor rotationPolicy (\p' st -> st { rotationPolicy = p' })

available :: forall r. Accessor (ResourceQueue r) (Seq r)
available = accessor _available (\q' st -> st { _available = q' })

busy :: forall r. Accessor (ResourceQueue r) (Set r)
busy = accessor _busy (\s' st -> st { _busy = s' })
