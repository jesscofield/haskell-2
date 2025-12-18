{-# LANGUAGE BangPatterns #-}

module Main where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar
import Control.Concurrent.STM
import Control.Monad (forM_, when, replicateM)
import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IM
import Data.List (foldl')
import System.Environment (getArgs)
import Data.Time.Clock (getCurrentTime, diffUTCTime)

type Graph = IntMap [(Int, Int)]

type PQ = IntMap [Int]

pqInsert :: Int -> Int -> PQ -> PQ
pqInsert d v = IM.insertWith (++) d [v]

pqPopMin :: PQ -> Maybe ((Int, Int), PQ)
pqPopMin pq = case IM.minViewWithKey pq of
  Nothing -> Nothing
  Just ((d, vs), pqRest) ->
    case vs of
      []     -> pqPopMin pqRest
      (v:xs) ->
        let pq' = if null xs then pqRest else IM.insert d xs pqRest
        in Just ((d, v), pq')

buildGraph :: [(Int, Int, Int)] -> Graph
buildGraph edges =
  foldl' (\g (u,v,w) -> IM.insertWith (++) u [(v,w)] g) IM.empty edges

parallelDijkstra :: Int -> Graph -> Int -> Int -> IO (Maybe Int)
parallelDijkstra nThreads g s t = do
  pqVar   <- newTVarIO (pqInsert 0 s IM.empty)
  distVar <- newTVarIO (IM.singleton s 0)
  doneVar <- newTVarIO False

  let worker :: IO ()
      worker = do
        done <- atomically $ readTVar doneVar
        if done then pure () else do
          mItem <- atomically $ do
            done' <- readTVar doneVar
            if done' then pure Nothing else do
              pq <- readTVar pqVar
              case pqPopMin pq of
                Nothing -> pure Nothing
                Just (item, pq') -> writeTVar pqVar pq' >> pure (Just item)

          case mItem of
            Nothing -> worker
            Just (d, v) -> do
              when (v == t) $
                atomically $ writeTVar doneVar True

              fresh <- atomically $ do
                dist <- readTVar distVar
                pure (IM.lookup v dist == Just d)

              when fresh $ do
                let neigh = IM.findWithDefault [] v g
                forM_ neigh $ \(to,w) -> do
                  let nd = d + w
                  atomically $ do
                    dist <- readTVar distVar
                    case IM.lookup to dist of
                      Nothing -> do
                        writeTVar distVar (IM.insert to nd dist)
                        modifyTVar' pqVar (pqInsert nd to)
                      Just old ->
                        when (nd < old) $ do
                          writeTVar distVar (IM.insert to nd dist)
                          modifyTVar' pqVar (pqInsert nd to)

              worker

  doneVars <- replicateM nThreads newEmptyMVar

  forM_ doneVars $ \mv ->
    forkIO (worker >> putMVar mv ())

  mapM_ takeMVar doneVars

  dist <- readTVarIO distVar
  pure (IM.lookup t dist)

readInput :: String -> ([(Int,Int,Int)], Int, Int)
readInput txt =
  let ws = words txt
      n  = read (ws !! 0) :: Int
      m  = read (ws !! 1) :: Int
      edgeNums = take (3*m) (drop 2 ws)
      edges = goEdges edgeNums
      s = read (ws !! (2 + 3*m))
      t = read (ws !! (3 + 3*m))
  in (edges, s, t)
  where
    goEdges [] = []
    goEdges (a:b:c:rest) = (read a, read b, read c) : goEdges rest
    goEdges _ = error "Bad edges format"

main :: IO ()
main = do
  args <- getArgs
  case args of
    [file, threadsStr] -> do
      content <- readFile file
      let (edges, s, t) = readInput content
          g = buildGraph edges
          threads = read threadsStr
      start <- getCurrentTime
      ans <- parallelDijkstra threads g s t
      end <- getCurrentTime
      putStrLn $ "distance: " ++ show ans
      putStrLn $ "threads:  " ++ show threads
      putStrLn $ "time:     " ++ show (diffUTCTime end start)
    _ ->
      putStrLn "Usage: dijkstra-par <graph.txt> <threads>   (run with +RTS -N...)"