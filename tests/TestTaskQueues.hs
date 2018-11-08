{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TupleSections       #-}

module Main where

import Control.Distributed.Process hiding (call, catch)
import Control.Distributed.Process.Async
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Extras.Time
import Control.Distributed.Process.Extras.Timer
import Control.Distributed.Process.Extras.Internal.Types
import Control.Distributed.Process.Node
import Control.Distributed.Process.Management
  ( MxAgentId(..)
  , MxEvent(MxRegistered)
  , mxAgent
  , mxSink
  , mxReady
  , liftMX
  )
import Control.Distributed.Process.ManagedProcess
import Control.Distributed.Process.Serializable()
import Control.Distributed.Process.SysTest.Utils

import Control.Distributed.Process.Task.Queue.BlockingQueue hiding (start)
import qualified Control.Distributed.Process.Task.Queue.BlockingQueue as Pool (start)

import Test.Framework (Test, defaultMain, testGroup)
import Test.Framework.Providers.HUnit (testCase)
-- import TestUtils

import Network.Transport.TCP
import qualified Network.Transport as NT

-- utilities

sampleTask :: (TimeInterval, String) -> Process String
sampleTask (t, s) = sleep t >> return s

namedTask :: (String, String) -> Process String
namedTask (name, result) = do
  self <- getSelfPid
  register name self
  () <- expect
  return result

crashingTask :: SendPort ProcessId -> Process String
crashingTask sp = getSelfPid >>= sendChan sp >> die "Boom"

$(remotable ['sampleTask, 'namedTask, 'crashingTask])

waitForDown :: MonitorRef -> Process DiedReason
waitForDown ref =
  receiveWait [ matchIf (\(ProcessMonitorNotification ref' _ _) -> ref == ref')
                        (\(ProcessMonitorNotification _ _ dr) -> return dr) ]

-- SimplePool tests

startPool :: SizeLimit -> Process ProcessId
startPool sz = spawnLocal $ do
  Pool.start (mkQueue sz :: Process (InitResult (BlockingQueue String)))

testSimplePoolJobBlocksCaller :: TestResult (AsyncResult (Either ExitReason String))
                              -> Process ()
testSimplePoolJobBlocksCaller result = do
  pid <- startPool 1
  -- we do a non-blocking test first
  job <- return $ ($(mkClosure 'sampleTask) (seconds 2, "foobar")) :: Process (Closure (Process String))
  call pid job >>= stash result . AsyncDone

testJobQueueSizeLimiting ::
    TestResult ((AsyncResult (Either ExitReason String)),
                (AsyncResult (Either ExitReason String)))
                         -> Process ()
testJobQueueSizeLimiting result = do
  (sp1, rp1) <- newChan
  (sp2, rp2) <- newChan
  listener <- mxAgent (MxAgentId "test.listener") () [
                        (mxSink $ \(ev :: MxEvent) -> do
                          case ev of
                            MxRegistered _ name -> do
                              case name of
                                "job1" -> liftMX (sendChan sp1 ()) >> mxReady
                                "job2" -> liftMX (sendChan sp2 ()) >> mxReady
                                _      -> mxReady
                            _                 -> mxReady)
                          ]

  pid <- startPool 1
  job1 <- return $ ($(mkClosure 'namedTask) ("job1", "foo"))
  job2 <- return $ ($(mkClosure 'namedTask) ("job2", "bar"))
  h1 <- callAsync pid job1 :: Process (Async (Either ExitReason String))
  m1 <- monitorAsync h1

  -- we can get here *very* fast, so give the registration time to kick in
  () <- receiveChan rp1
  Just p <- whereis "job1"

  h2 <- callAsync pid job2 :: Process (Async (Either ExitReason String))
  m2 <- monitorAsync h2

  -- despite the fact that we tell job2 to proceed first,
  -- the size limit (of 1) will ensure that only job1 can
  -- proceed successfully!
  AsyncPending <- poll h2
  Nothing <- whereis "job2"

  send p ()
  waitForDown m1

  -- once job1 completes, we *should* be able to proceed with job2
  -- but we allow a little time for things to catch up
  () <- receiveChan rp2
  Just p2 <- whereis "job2"
  send p2 ()

  waitForDown m2

  monitor p >>= waitForDown
  monitor p2 >>= waitForDown

  r2' <- wait h2
  r1' <- wait h1
  stash result (r1', r2')
  kill listener "done"

testExecutionErrors :: TestResult Bool -> Process ()
testExecutionErrors result = do
    pid <- startPool 1
    (sp, rp) <- newChan :: Process (SendPort ProcessId,
                                    ReceivePort ProcessId)
    job <- return $ ($(mkClosure 'crashingTask) sp)
    res <- executeTask pid job
    rpid <- receiveChan rp
--  liftIO $ putStrLn (show res)
    stash result (expectedErrorMessage rpid == res)
  where
    expectedErrorMessage p =
      Left $ ExitOther $ "DiedException \"exit-from=" ++ (show p) ++ ",reason=Boom\""

myRemoteTable :: RemoteTable
myRemoteTable = Main.__remoteTable initRemoteTable

tests :: NT.Transport  -> IO [Test]
tests transport = do
  localNode <- newLocalNode transport myRemoteTable
  return [
    testGroup "Task Execution And Prioritisation" [
         testCase "Each execution blocks the submitter"
         (delayedAssertion
          "expected the server to return the task outcome"
          localNode (AsyncDone (Right "foobar")) testSimplePoolJobBlocksCaller)
       , testCase "Only 'max' tasks can proceed at any time"
         (delayedAssertion
          "expected the server to block the second job until the first was released"
          localNode
          ((AsyncDone (Right "foo")),
           (AsyncDone (Right "bar"))) testJobQueueSizeLimiting)
       , testCase "Crashing Tasks are Reported Properly"
         (delayedAssertion
          "expected the server to report an error"
          localNode True testExecutionErrors)
       ]
    ]

main :: IO ()
main = testMain $ tests

-- | Given a @builder@ function, make and run a test suite on a single transport
testMain :: (NT.Transport -> IO [Test]) -> IO ()
testMain builder = do
  Right (transport, _) <- createTransportExposeInternals
                            "127.0.0.1" "0" ("127.0.0.1",) defaultTCPParameters
  testData <- builder transport
  defaultMain testData
