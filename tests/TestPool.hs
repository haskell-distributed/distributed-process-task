{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable  #-}
{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE TemplateHaskell     #-}

module Main where

import Control.Exception (SomeException)
import Control.Distributed.Process hiding (call, catch)
import Control.Distributed.Process.Async
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Extras (__remoteTable)
import Control.Distributed.Process.Extras.Time
import Control.Distributed.Process.Extras.Timer
import Control.Distributed.Process.Extras.Internal.Primitives (forever')
import Control.Distributed.Process.Extras.Internal.Types
import Control.Distributed.Process.Node
import Control.Distributed.Process.SysTest.Utils
import Control.Distributed.Process.Task.Pool hiding (start)
import qualified Control.Distributed.Process.Task.Pool as Pool (start)
import Control.Distributed.Process.Task.Pool.WorkerPool
 ( runWorkerPool
 , Worker
 )
import Control.Rematch (equalTo)
import Control.Monad (replicateM)
import Control.Monad.Catch (catch)
import Data.List
 ( elemIndex
 )
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
 ( empty
 , insert
 , lookup
 )

import Test.Framework (Test, defaultMain, testGroup)
import Test.Framework.Providers.HUnit (testCase)
-- import TestUtils

import Network.Transport.TCP
import qualified Network.Transport as NT

testProcess :: SendPort () -> ReceivePort () -> Process ()
testProcess sp rp = do
  liftIO $ putStrLn "worker starting up"
  sendChan sp () >> go
  where
    go = do
      cmd <- receiveWait [ matchChan rp (return . Right)
                         , matchAny (return . Left)
                         ]
      case cmd of
        Right () -> return ()
        Left  m  -> do handleMessage_ m (\(r :: ExitReason)  -> die r)
                       handleMessage_ m (\(pid :: ProcessId) -> send pid ())
                       go

testPoolBackend :: TestResult () -> Process ()
testPoolBackend result = do
  (sp, rp) <- newChan
  (sig, rSig) <- newChan
  let poolStart = runWorkerPool (testProcess sig rp) 1 OnDemand LRU Destroy

  pool <- Pool.start poolStart :: Process (ResourcePool Worker)

  liftIO $ putStrLn "Pool started"

  -- no worker should've started yet
  thing <- catch (receiveChanTimeout (asTimeout $ seconds 2) rSig) (\(ex :: SomeException) -> (liftIO $ putStrLn (show ex)) >> terminate)

  liftIO $ putStrLn $ "Thing = " ++ (show thing)

  thing `shouldBe` equalTo Nothing

  -- OnDemand means we'll see a worker process start up
  w1 <- catchExit (acquireResource pool) (\_ (er :: ExitReason) -> (liftIO $ putStrLn (show er)) >> terminate)

  liftIO $ putStrLn $ "w1 = " ++ (show w1)

  -- which means we'll get a signal
  () <- receiveChan rSig

  -- sending our pid means we can expect a reply
  getSelfPid >>= sendTo w1
  () <- expect

  -- the pool should keep us hanging on if we request a resource at this point
  let acquireAsync = async $ task $ acquireResource pool :: Process (Async Worker)

  res <- waitCancelTimeout (asTimeout $ seconds 3) =<< acquireAsync
  res `shouldBe` equalTo (AsyncCancelled :: AsyncResult Worker)

  liftIO $ putStrLn "Cancellation worked as expected"

  hAsync <- acquireAsync
  res' <- waitCheckTimeout (asTimeout $ seconds 3) hAsync
  res' `shouldBe` equalTo (AsyncPending :: AsyncResult Worker)

  liftIO $ putStrLn "Pending set as expected"

  -- if we make the first worker terminate, we should have access to the second...
  sendChan sp ()

  -- and we see acquireResource complete on the second request...
  Just (AsyncDone w2) <- waitTimeout (asTimeout $ seconds 5) hAsync

  liftIO $ putStrLn $ "w2 = " ++ (show w2)

  -- which means we'll get a start signal again
  () <- receiveChan rSig

  killProc w2 "bye bye"

  stash result ()

-- utilities
myRemoteTable :: RemoteTable
myRemoteTable =
  Control.Distributed.Process.Extras.__remoteTable initRemoteTable

tests :: NT.Transport  -> IO [Test]
tests transport = do
  localNode <- newLocalNode transport myRemoteTable
  return [
    testGroup "Basic Pool Behaviour" [
         testCase "Pool API Handoff To Backend"
         (delayedAssertion
          "expected the backend to manage API calls"
          localNode () testPoolBackend)
       ]
    ]

main :: IO ()
main = testMain $ tests

-- | Given a @builder@ function, make and run a test suite on a single transport
testMain :: (NT.Transport -> IO [Test]) -> IO ()
testMain builder = do
  Right (transport, _) <- createTransportExposeInternals
                                    "127.0.0.1" "0" defaultTCPParameters
  testData <- builder transport
  defaultMain testData
