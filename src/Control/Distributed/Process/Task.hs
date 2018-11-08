-----------------------------------------------------------------------------
-- |
-- Module      :  Control.Distributed.Process.Platform.Task
-- Copyright   :  (c) Tim Watson 2013 - 2014
-- License     :  BSD3 (see the file LICENSE)
--
-- Maintainer  :  Tim Watson <watson.timothy@gmail.com>
-- Stability   :  experimental
-- Portability :  non-portable (requires concurrency)
--
-- A Cloud Haskell /Task Framework/, providing tools for task management, work
-- scheduling and distributed task coordination. These capabilities build on the
-- /Execution Framework/ as well as other tools and libraries.
--
-- The /Task Framework/ is broken down by the task scheduling and management
-- algorithms it provides, e.g., at a low level providing work queues, worker pools
-- and the like, whilst at a high level allowing the user to choose between work
-- stealing, sharing, distributed coordination, user defined sensor based
-- bounds/limits, and so on.
--
-----------------------------------------------------------------------------
module Control.Distributed.Process.Task
  ( -- * Task Queues
    module Control.Distributed.Process.Task.Queue.BlockingQueue
  ) where

import Control.Distributed.Process.Task.Queue.BlockingQueue
