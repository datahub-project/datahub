#! /usr/bin/env nix-shell
#! nix-shell ./lineage_hive_generator.hs.nix -i runghc

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE QuasiQuotes #-}

import Data.Functor ((<&>))
import Control.Monad (when)
import Control.Arrow ((>>>))
import Data.Proxy (Proxy(..))
import Data.Either (isLeft, fromLeft, fromRight)

import Text.Printf (formatString)

import System.IO (hPrint, stderr)

import Data.String.Conversions (cs)
import qualified Data.Text.Lazy as T
import qualified Data.Text.Lazy.IO as T

import qualified Data.Map as M
import qualified Data.Set as S
import qualified Data.HashMap.Strict as HM
import qualified Data.Aeson as J

import Data.Conduit (ConduitT, runConduitRes, runConduit, bracketP, (.|))
import qualified Data.Conduit.Combinators as C

import qualified Database.Sql.Hive.Parser as HIVE
import qualified Database.Sql.Hive.Type as HIVE

import Database.Sql.Type (
    Catalog(..), DatabaseName(..), FullyQualifiedTableName(..), FQTN(..)
  , makeDefaultingCatalog, mkNormalSchema
  )

import Database.Sql.Util.Scope (runResolverWarn)
import Database.Sql.Util.Lineage.Table (getTableLineage)

import Data.Aeson.QQ (aesonQQ)
import Data.Time.Clock.POSIX (getPOSIXTime)


instance J.ToJSON FullyQualifiedTableName
instance J.ToJSONKey FullyQualifiedTableName

nowts :: IO Int
nowts = floor . (* 1000) <$> getPOSIXTime

catalog :: Catalog
catalog = makeDefaultingCatalog HM.empty
                                [mkNormalSchema "public" ()]
                                (DatabaseName () "defaultDatabase")

tableName :: FullyQualifiedTableName -> T.Text
tableName (FullyQualifiedTableName database schema name) = T.intercalate "." [database, schema, name]

mkMCE :: Int -> (FQTN, S.Set FQTN) -> J.Value
mkMCE ts (output, inputs) = [aesonQQ|
  { "proposedSnapshot": {
      "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
        "urn": #{uriName output}
      , "aspects": [
          { "com.linkedin.pegasus2avro.dataset.UpstreamLineage": {
              "upstreams": #{upstreams ts inputs}
            }
          }
        ]
      }
    }
  }
  |]
  where
    upstream :: Int -> T.Text -> J.Value
    upstream ts dataset = [aesonQQ|
      { "auditStamp": {"time":#{ts}, "actor":"urn:li:corpuser:jdoe"}
      , "dataset": #{dataset}
      , "type":"TRANSFORMED"
      }
    |]
    upstreams ts = map (upstream ts . uriName) . S.toList
    uriName :: FQTN -> T.Text
    uriName fqtn = "urn:li:dataset:(urn:li:dataPlatform:hive,"
                   <> tableName fqtn
                   <> ",PROD)"

  
main = do 
  contents <- T.getContents <&> T.lines
  ts <- nowts

  runConduit $ C.yieldMany contents
            .| C.iterM (hPrint stderr)
            .| C.mapM (cs >>> T.readFile)
            .| C.concatMap parseSQL
            .| C.mapM resolveStatement
            .| C.concatMap (getTableLineage >>> M.toList)
            .| C.map (mkMCE ts)
            .| C.mapM_ (J.encode >>> cs >>> putStrLn)
  where
    parseSQL sql = do
      let stOrErr = HIVE.parseManyAll sql
      when (isLeft stOrErr) $
        error $ show (fromLeft undefined stOrErr)
      fromRight undefined stOrErr
    resolveStatement st =  do
      let resolvedStOrErr = runResolverWarn (HIVE.resolveHiveStatement st) HIVE.dialectProxy catalog
      when (isLeft . fst $ resolvedStOrErr) $
        error $ show (fromLeft undefined (fst resolvedStOrErr))
      let (Right queryResolved, resolutions) = resolvedStOrErr
      return queryResolved

