{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE PatternGuards              #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE UndecidableInstances       #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Task.Pool.Internal.Process
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
-- [Pool Process Implementation]
--
-- This module implements the /Managed Process/ part of the Pool API.
-- See "Control.Distributed.Process.Task.Pool".
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Task.Pool.Internal.Process
 ( -- Server State, Management & Lenses
   State
 , Ticket(..)
 , defaultState
 , poolBackend
 , poolState
 , clients
 , monitors
 , pending
 , locked
   -- Starting/Running a Resource Pool
 , runPool
 , poolInit
 , runPoolStateT
   -- Process Definition
 , processDefinition
 ) where

import Control.Distributed.Process
  ( Process
  , MonitorRef
  , ProcessMonitorNotification(..)
  , ProcessId
  , SendPort
  , unsafeSendChan
  , Message
  , monitor
  , unmonitor
  , unsafeWrapMessage
  , liftIO
  )
import Control.Distributed.Process.Extras.Internal.Queue.SeqQ (SeqQ)
import qualified Control.Distributed.Process.Extras.Internal.Queue.SeqQ as Queue
import Control.Distributed.Process.Extras.Internal.Containers.MultiMap (MultiMap)
import qualified Control.Distributed.Process.Extras.Internal.Containers.MultiMap as MultiMap
import Control.Distributed.Process.Extras.Internal.Types
  ( ExitReason(..)
  )
import Control.Distributed.Process.ManagedProcess
  ( handleInfo
  , handleRaw
  , handleRpcChan
  , handleCast
  , handleCall
  , continue
  , reply
  , defaultProcess
  , exitState
  , InitHandler
  , CallHandler
  , InitResult(..)
  , ProcessAction
  , ProcessDefinition(..)
  , UnhandledMessagePolicy(..)
  , ExitState
  )
import qualified Control.Distributed.Process.ManagedProcess as MP
  ( serve
  )
import Control.Distributed.Process.Extras.Time
import Control.Distributed.Process.Serializable
import Control.Distributed.Process.Task.Pool.Internal.Types
import Control.Monad (void, forM_)
import qualified Control.Monad.State as ST
 ( runStateT
 )
import Data.Accessor
 ( Accessor
 , accessor
 , (^:)
 , (^=)
 , (.>)
 , (^.)
 )
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
 ( empty
 , insert
 , lookup
 , delete
 )
import qualified Data.Sequence as Seq (length)
import Data.Set (Set)
import Data.HashSet (HashSet)
import qualified Data.HashSet as HashSet
import qualified Data.Set as Set

--------------------------------------------------------------------------------
-- Server State, Management & Lenses                                          --
--------------------------------------------------------------------------------

data State s r = State { _pool      :: PoolBackend s r
                       , _poolState :: PoolState s r
                       , _clients   :: MultiMap ProcessId r
                       , _monitors  :: Map ProcessId MonitorRef
                       , _pending   :: SeqQ (Ticket r)
                       , _locked    :: Set r
                       , reclaim    :: ReclamationStrategy
                       }

defaultState :: forall r s. (Referenced r)
             => Resource r
             -> PoolBackend s r
             -> InitPolicy
             -> RotationPolicy s r
             -> ReclamationStrategy
             -> s
             -> State s r
defaultState rt pl ip rp rs ps = State { _pool      = pl
                                       , _poolState = initialPoolState rt ip rp ps
                                       , _clients   = MultiMap.empty
                                       , _monitors  = Map.empty
                                       , _pending   = Queue.empty
                                       , _locked    = Set.empty
                                       , reclaim    = rs
                                       }

poolBackend :: forall s r. Accessor (State s r) (PoolBackend s r)
poolBackend = accessor _pool (\pl' st -> st { _pool = pl' })

poolState :: forall s r. Accessor (State s r) (PoolState s r)
poolState = accessor _poolState (\ps' st -> st { _poolState = ps' })

clients :: forall s r. Accessor (State s r) (MultiMap ProcessId r)
clients = accessor _clients (\cs' st -> st { _clients = cs' })

monitors :: forall s r. Accessor (State s r) (Map ProcessId MonitorRef)
monitors = accessor _monitors (\ms' st -> st { _monitors = ms' })

pending :: forall s r. Accessor (State s r) (SeqQ (Ticket r))
pending = accessor _pending (\tks' st -> st { _pending = tks' })

locked :: forall s r. Accessor (State s r) (Set r)
locked = accessor _locked (\lkd' st -> st { _locked = lkd' })

--------------------------------------------------------------------------------
-- Starting/Running a Resource Pool                                           --
--------------------------------------------------------------------------------

-- | Run the resource pool server loop.
--
runPool :: forall s r. (Referenced r)
        => Resource r
        -> PoolBackend s r
        -> InitPolicy
        -> RotationPolicy s r
        -> ReclamationStrategy
        -> s
        -> Process ()
runPool t p i r d s =
  MP.serve (t, p, i, r, d, s) poolInit (processDefinition :: ProcessDefinition (State s r))

poolInit :: forall s r . (Referenced r)
         => InitHandler (Resource r, PoolBackend s r,
                         InitPolicy, RotationPolicy s r,
                         ReclamationStrategy, s) (State s r)
poolInit (rt, pb, ip, rp, rs, ps) =
  let st  = defaultState rt pb ip rp rs ps
      ps' = st ^. poolState
  in do ((), pState) <- runPoolStateT ps' (setup pb)
        return $ InitOk ((poolState ^= pState) $ st) Infinity

runPoolStateT :: PoolState s r -> Pool s r a -> Process (a, PoolState s r)
runPoolStateT state proc = ST.runStateT (unPool proc) state

{-
      case lState of
        Block ls -> do lState' <- reset limiter ls
                       return $ ((limiterState ^= lState') $ st)
        Take  ls -> do let (wId, pool') = takeId _workerIds
                       ref <- create resourceType wId
                       startWorkers $ ( (workers ^: Map.insert ref wId)
                                      . (workerIds ^= pool')
                                      . (limiterState ^= ls)
                                      $ st
                                      )
-}

--------------------------------------------------------------------------------
-- Process Definition/State & API Handlers                                    --
--------------------------------------------------------------------------------

processDefinition :: forall s r.
                     (Referenced r)
                  => ProcessDefinition (State s r)
processDefinition =
  defaultProcess { apiHandlers     = [ handleRpcChan handleAcquire
                                     , handleCast    handleRelease
                                     , handleCall    handleGetStats
                                     ]
                 , infoHandlers    = [ handleInfo handleMonitorSignal
                                     , handleRaw  backendInfoCall
                                     ]
                 , shutdownHandler = handleShutdown
                 }

handleAcquire :: forall s r. (Referenced r)
              => SendPort r
              -> State s r
              -> AcquireResource
              -> Process (ProcessAction (State s r))
handleAcquire clientPort st@State{..} (AcquireResource clientPid) = do
  liftIO $ putStrLn "acquiring resource"
  mRef <- monitor clientPid
  (take', pst) <- runPoolStateT (st ^. poolState) (acquire (st ^. poolBackend))
  liftIO $ putStrLn $ "acquired " ++ (show take')
  let st' = ( (poolState ^= pst)
            . (monitors ^: Map.insert clientPid mRef)
            $ st )
  case take' of
    Block  -> addPending clientPid clientPort st'
    Take r -> allocateResource r (Ticket clientPid clientPort) st'

handleRelease :: forall s r. (Referenced r)
              => State s r
              -> ReleaseResource r
              -> Process (ProcessAction (State s r))
handleRelease st@State{..} (ReleaseResource res pid) = do
  maybe (return ()) (void . unmonitor) $ Map.lookup pid (st ^. monitors)
  (_, pst) <- runPoolStateT (st ^. poolState) (release (st ^. poolBackend) res)
  dequeuePending $ ( (clients ^: MultiMap.filter (/= res))
                   . (poolState ^= pst)
                   $ st )

handleGetStats :: forall s r . (Referenced r)
               => CallHandler (State s r) StatsReq PoolStats
handleGetStats st StatsReq = do
  (info, pst) <- runPoolStateT (st ^. poolState) $ getStats (st ^. poolBackend)
  let (a, b) = lengths (st ^. poolState .> resourceQueue)
  reply PoolStats { totalResources    = (toInteger $ a + b)
                  , activeResources   = b
                  , inactiveResources = a
                  , activeClients     = MultiMap.size (st ^. clients)
                  , pendingClients    = Queue.size    (st ^. pending)
                  , backendStats      = info
                  } $ ((poolState ^= pst) st)
  where
    lengths (ResourceQueue a b) = (Seq.length a, Set.size b)

dequeuePending :: forall s r . (Referenced r)
               => State s r
               -> Process (ProcessAction (State s r))
dequeuePending st@State{..} = do
  case Queue.dequeue pq of
    Just (t, pending') -> bumpTicket t pending' st
    Nothing            -> continue st
  where
    pq = st ^. pending

bumpTicket :: forall s r. (Referenced r)
           => Ticket r
           -> SeqQ (Ticket r)
           -> State s r
           -> Process (ProcessAction (State s r))
bumpTicket t pd st = do
  -- Although we've just released a resource, there's no guarantee that
  -- the pool backend will let us acquire another one, so we need to
  -- cater for both cases here.
  (take', pst) <- runPoolStateT (st ^. poolState) (acquire (st ^. poolBackend))
  case take' of
    Block  -> continue st
    Take r -> allocateResource r t $ ( (poolState ^= pst)
                                     . (pending ^= pd)
                                     $ st )

handleMonitorSignal :: forall s r. (Referenced r)
                    => State s r
                    -> ProcessMonitorNotification
                    -> Process (ProcessAction (State s r))
handleMonitorSignal st mSig@(ProcessMonitorNotification _ pid _) = do
  let st' = (monitors ^: Map.delete pid) st
  let mp = MultiMap.delete pid (st' ^. clients)
  case mp of
    Nothing       -> handoffToBackend mSig =<< clearTickets pid st
    Just (rs, q') -> applyReclamationStrategy pid (HashSet.fromList rs) $ (clients ^= q') st'

handleShutdown :: forall s r . ExitState (State s r) -> ExitReason -> Process ()
handleShutdown es reason
  | st <- exitState es =
      void $ runPoolStateT (st ^. poolState) (teardown (st ^. poolBackend) $ reason)

backendInfoCall :: forall s r . (Referenced r)
                => State s r
                -> Message
                -> Process (ProcessAction (State s r))
backendInfoCall st msg = do
  ((), pst) <- runPoolStateT ps $ infoCall pl msg
  let act = if (isDirty pst) then dequeuePending else continue
  act $ (poolState ^= pst) st
  where
    ps = st ^. poolState
    pl = st ^. poolBackend

handoffToBackend :: forall s r m. (Referenced r, Serializable m)
                 => m
                 -> State s r
                 -> Process (ProcessAction (State s r))
handoffToBackend msg st = backendInfoCall st (unsafeWrapMessage msg)

clearTickets :: forall s r. (Referenced r)
             => ProcessId
             -> State s r
             -> Process (State s r)
clearTickets pid st@State{..} = do
  return $ (pending ^: Queue.filter ((/= pid) . ticketOwner)) st
  {-
  let sz' = Queue.size $ st' ^. pending
  case (sz, sz') of
    _ | sz > sz'  = return $ (stats .> waiting ^: (-(sz - sz'))) $ st'
      | sz == sz' = return st'
      | otherwise = die $ ExitOther $ baseErr ++ ":clearTickets-ResizeInvalid"
  -}

baseErr :: String
baseErr = baseErrorMessage ++ ".Internal.Process"

applyReclamationStrategy :: forall s r . (Referenced r)
                         => ProcessId
                         -> HashSet r
                         -> State s r
                         -> Process (ProcessAction (State s r))
applyReclamationStrategy pid rs st@State{..} =
  let act = case reclaim of
              Release  -> doRelease
              Destroy  -> doDestroy
              PermLock -> doPermLock
  in clearTickets pid st >>= act rs >>= handoffToBackend pid
  where
    doRelease rs' st' = do
      (_, pst) <- runPoolStateT poolState' (forM_ (HashSet.toList rs') release')
      return $ ( (poolState ^= pst)
               $ st' )

    doDestroy rs' st' = do
      (_, pst) <- runPoolStateT poolState' (forM_ (HashSet.toList rs') dispose')
      return $ ( (poolState ^= pst)
               $ st' )

    doPermLock rs' st' = do
      -- we mark resources as locked, but to what end? isn't it sufficient to
      -- just /ignore/ the client's death and leave those resources marked (in
      -- the pool backend) as @busy@ here instead? When do we ever check this
      -- /locked list/ anyway???
      return $ ( (locked ^: Set.union (Set.fromList (HashSet.toList rs'))) st' )

    poolState'    = st ^. poolState
    dispose'      = dispose (st ^. poolBackend)
    release'      = release (st ^. poolBackend)

addPending :: forall s r . (Referenced r)
           => ProcessId
           -> SendPort r
           -> State s r
           -> Process (ProcessAction (State s r))
addPending clientPid clientPort st = do
  let st' = ( (pending ^: \q -> Queue.enqueue q $ Ticket clientPid clientPort)
            $ st)
  continue st'

allocateResource :: forall s r. (Referenced r)
                 => r
                 -> Ticket r
                 -> State s r
                 -> Process (ProcessAction (State s r))
allocateResource ref ticket@Ticket{..} st = do
  let rType = resourceType $ st ^. poolState
  accept rType ticket ref
  continue $ (clients ^: MultiMap.insert ticketOwner ref) $ st
