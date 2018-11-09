{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE DeriveGeneric             #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Task.Queue.BlockingQueue
-- Copyright   :  (c) Tim Watson 2012 - 2013
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- A simple bounded (size) task queue, which accepts requests and blocks the
-- sender until they're completed. The size limit is applied to the number
-- of concurrent tasks that are allowed to execute - if the limit is 3, then
-- the first three tasks will be executed immediately, but further tasks will
-- then be queued (internally) until one or more tasks completes and
-- the number of active/running tasks falls within the concurrency limit.
--
-- Note that the process calling 'executeTask' will be blocked for _at least_
-- the duration of the task itself, regardless of whether or not the queue has
-- reached its concurrency limit. This provides a simple means to prevent work
-- from being submitted faster than the server can handle, at the expense of
-- flexible scheduling.
--
-----------------------------------------------------------------------------

module Control.Distributed.Process.Task.Queue.BlockingQueue
  ( BlockingQueue()
  , SizeLimit
  , BlockingQueueStats(..)
  , startQueue
  , mkQueue
  , executeTask
  , queueStats
  ) where

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure()
import Control.Distributed.Process.Extras.Internal.Types
import Control.Distributed.Process.Async
import Control.Distributed.Process.ManagedProcess
 ( InitResult(InitOk)
 , ProcessDefinition(..)
 , ProcessReply
 , ProcessAction
 , CallRef
 , handleCall
 , handleCallFrom
 , handleInfo
 , defaultProcess
 , tryCall
 , call
 , continue
 , reply
 , noReply_
 , replyTo
 )
import qualified Control.Distributed.Process.ManagedProcess as ManagedProcess
import Control.Distributed.Process.Extras.Time
import Control.Distributed.Process.Serializable
import Data.Binary
import Data.List
  ( deleteBy
  , find
  )
import Data.Sequence
  ( Seq
  , ViewR(..)
  , (<|)
  , viewr
  )
import qualified Data.Sequence as Seq (empty, length)
import Data.Typeable

import GHC.Generics (Generic)

-- | Limit for the number of concurrent tasks.
--
type SizeLimit = Int

data GetStats = GetStats
  deriving (Typeable, Generic)
instance Binary GetStats where

-- | Statistics for a blocking queue.
data BlockingQueueStats = BlockingQueueStats {
    maxJobs    :: Int
  , activeJobs :: Int
  , queuedJobs :: Int
  } deriving (Typeable, Generic)

instance Binary BlockingQueueStats where

data BlockingQueue a = BlockingQueue {
    poolSize :: SizeLimit
  , active   :: [(MonitorRef, CallRef (Either ExitReason a), Async a)]
  , accepted :: Seq (CallRef (Either ExitReason a), Closure (Process a))
  } deriving (Typeable)

-- Client facing API

-- | Start a queue with an upper bound on the # of concurrent tasks.
--
startQueue :: forall a . (Serializable a)
           => Process (InitResult (BlockingQueue a))
           -> Process ()
startQueue init' = ManagedProcess.serve () (\() -> init') poolServer
  where poolServer =
          defaultProcess {
              apiHandlers = [
                 handleCallFrom storeTask
               , handleCall     poolStatsRequest
               ]
            , infoHandlers = [ handleInfo taskComplete ]
            } :: ProcessDefinition (BlockingQueue a)

-- | Define a pool of a given size.
--
mkQueue :: forall a . Serializable a
     => SizeLimit
     -> Process (InitResult (BlockingQueue a))
mkQueue sz' = return $ InitOk (BlockingQueue sz' [] Seq.empty) Infinity

-- | Enqueue a task in the pool and block until it is complete.
--
executeTask :: forall s a . (Addressable s, Serializable a)
            => s
            -> Closure (Process a)
            -> Process (Either ExitReason a)
executeTask = call

-- | Fetch statistics for a queue.
--
queueStats :: forall s . Addressable s => s -> Process (Maybe BlockingQueueStats)
queueStats sid = tryCall sid GetStats

-- internal / server-side API

poolStatsRequest :: (Serializable a)
                 => BlockingQueue a
                 -> GetStats
                 -> Process (ProcessReply BlockingQueueStats (BlockingQueue a))
poolStatsRequest st GetStats =
  let sz = poolSize st
      ac = length (active st)
      pj = Seq.length (accepted st)
  in reply (BlockingQueueStats sz ac pj) st

storeTask :: Serializable a
          => CallRef (Either ExitReason a)
          -> BlockingQueue a
          -> Closure (Process a)
          -> Process (ProcessReply (Either ExitReason a) (BlockingQueue a))
storeTask r s c = acceptTask r s c >>= noReply_

acceptTask :: Serializable a
           => CallRef (Either ExitReason a)
           -> BlockingQueue a
           -> Closure (Process a)
           -> Process (BlockingQueue a)
acceptTask from s@(BlockingQueue sz' runQueue taskQueue) task' = do
  let currentSz = length runQueue
  if currentSz >= sz'
    then return s{ accepted = enqueue taskQueue (from, task') }
    else do proc <- unClosure task'
            asyncHandle <- async $ task $ expect >>= \() -> proc
            ref <- monitorAsync asyncHandle
            let wkr = asyncWorker asyncHandle
            let taskEntry = (ref, from, asyncHandle)
            send wkr ()
            return s { active = taskEntry:runQueue }

-- a worker has exited, process the AsyncResult and send a reply to the
-- waiting client (who is still stuck in 'call' awaiting a response).
taskComplete :: forall a . Serializable a
             => BlockingQueue a
             -> ProcessMonitorNotification
             -> Process (ProcessAction (BlockingQueue a))
taskComplete s@(BlockingQueue _ runQ _)
             (ProcessMonitorNotification ref _ _) =
  let worker = findWorker ref runQ in
  case worker of
    Just t@(_, c, h) -> wait h >>= respond c >> bump s t >>= continue
    Nothing          -> continue s

  where
    respond :: CallRef (Either ExitReason a)
            -> AsyncResult a
            -> Process ()
    respond c (AsyncDone       r) = replyTo c (Right r :: (Either ExitReason a))
    respond c (AsyncFailed     d) = replyTo c (Left (ExitOther $ show d)  :: (Either ExitReason a))
    respond c (AsyncLinkFailed d) = replyTo c (Left (ExitOther $ show d)  :: (Either ExitReason a))
    respond _      _              = die $ ExitOther "IllegalState"

    bump :: BlockingQueue a
         -> (MonitorRef, CallRef (Either ExitReason a), Async a)
         -> Process (BlockingQueue a)
    bump st@(BlockingQueue _ runQueue acc) worker =
      let runQ2 = deleteFromRunQueue worker runQueue
          accQ  = dequeue acc in
      case accQ of
        Nothing            -> return st { active = runQ2 }
        Just ((tr,tc), ts) -> acceptTask tr (st { accepted = ts, active = runQ2 }) tc

findWorker :: MonitorRef
           -> [(MonitorRef, CallRef (Either ExitReason a), Async a)]
           -> Maybe (MonitorRef, CallRef (Either ExitReason a), Async a)
findWorker key = find (\(ref,_,_) -> ref == key)

deleteFromRunQueue :: (MonitorRef, CallRef (Either ExitReason a), Async a)
                   -> [(MonitorRef, CallRef (Either ExitReason a), Async a)]
                   -> [(MonitorRef, CallRef (Either ExitReason a), Async a)]
deleteFromRunQueue c@(p, _, _) = deleteBy (\_ (b, _, _) -> b == p) c

{-# INLINE enqueue #-}
enqueue :: Seq a -> a -> Seq a
enqueue s a = a <| s

{-# INLINE dequeue #-}
dequeue :: Seq a -> Maybe (a, Seq a)
dequeue s = maybe Nothing (\(s' :> a) -> Just (a, s')) $ getR s

getR :: Seq a -> Maybe (ViewR a)
getR s =
  case viewr s of
    EmptyR -> Nothing
    a      -> Just a
