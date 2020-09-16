#! /usr/bin/env nix-shell
#! nix-shell dataset-jdbc-generator.hs.nix -i "runghc --ghc-arg=-fobject-code"

{-# LANGUAGE OverloadedStrings, FlexibleInstances, FlexibleContexts, ScopedTypeVariables #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TemplateHaskell, QuasiQuotes #-}
{-# LANGUAGE TypeOperators #-}


{-# OPTIONS_GHC -fplugin=Language.Java.Inline.Plugin #-}


import System.Environment (lookupEnv)
import System.IO (hPrint, stderr, hSetEncoding, stdout, utf8)

import qualified Language.Haskell.TH.Syntax as TH

import Control.Concurrent (runInBoundThread)
import Language.Java (J, withJVM, reify, reflect, JType(..))
import Language.Java.Inline (imports, java)

import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.String.Conversions (cs)
import Text.InterpolatedString.Perl6 (q)

import Prelude hiding ((>>=), (>>))

import Data.Conduit (ConduitT, ZipSink(..), getZipSink, runConduitRes, runConduit, bracketP, (.|), yield)
import qualified Data.Conduit.Combinators as C
import qualified Data.Conduit.List as C (groupBy)

import qualified Data.Aeson as J
import Control.Arrow ((>>>))
import Data.Aeson.QQ (aesonQQ)

import Text.Printf (printf)


imports "java.util.*"
imports "java.sql.*"


datasetOracleSql :: T.Text
datasetOracleSql = [q|
    select
      c.OWNER || '.' || c.TABLE_NAME as schema_name
    , t.COMMENTS as schema_description
    , c.COLUMN_NAME as field_path
    , c.DATA_TYPE as native_data_type 
    , m.COMMENTS as description
    from ALL_TAB_COLUMNS c
      left join ALL_TAB_COMMENTS t
        on c.OWNER = t.OWNER
        and c.TABLE_NAME = t.TABLE_NAME
      left join ALL_COL_COMMENTS m
        on c.OWNER = m.OWNER
        and c.TABLE_NAME = m.TABLE_NAME
        and c.COLUMN_NAME = m.COLUMN_NAME
    where NOT REGEXP_LIKE(c.OWNER, 'ANONYMOUS|PUBLIC|SYS|SYSTEM|DBSNMP|MDSYS|CTXSYS|XDB|TSMSYS|ORACLE.*|APEX.*|TEST?*|GG_.*|\\$')
    order by schema_name, c.COLUMN_ID
|]

datasetMysqlSql :: T.Text
datasetMysqlSql = [q|
    select 
      concat(c.TABLE_SCHEMA, '.', c.TABLE_NAME) as schema_name
    , NULL as schema_description
    , c.COLUMN_NAME as field_path
    , c.DATA_TYPE as native_data_type
    , c.COLUMN_COMMENT as description
    from information_schema.columns c
    where table_schema not in ('information_schema') 
    order by schema_name, c.ORDINAL_POSITION
|]

datasetPostgresqlSql :: T.Text
datasetPostgresqlSql = [q|
    SELECT
        c.table_schema || '.' || c.table_name as schema_name
      , pgtd.description as schema_description
      , c.column_name as field_path
      , c.data_type as native_data_type
      , pgcd.description as description
    FROM INFORMATION_SCHEMA.COLUMNS c
    INNER JOIN
      pg_catalog.pg_statio_all_tables as st on c.table_schema=st.schemaname and c.table_name=st.relname
    LEFT JOIN
      pg_catalog.pg_description pgcd on pgcd.objoid=st.relid and pgcd.objsubid=c.ordinal_position
    LEFT JOIN
      pg_catalog.pg_description pgtd on pgtd.objoid=st.relid and pgtd.objsubid=0
    WHERE c.table_schema NOT IN ('information_schema', 'pg_catalog')
    ORDER by schema_name, ordinal_position ;
|]


mkMCE :: Int -> T.Text -> [[T.Text]] -> J.Value
mkMCE ts platform fields@((schemaName:schemaDescription:_):_) = [aesonQQ|
  { "proposedSnapshot": {
      "com.linkedin.pegasus2avro.metadata.snapshot.DatasetSnapshot": {
        "urn": #{urn}
      , "aspects": [
          { "com.linkedin.pegasus2avro.common.Ownership": {
              "owners": [{"owner": "urn:li:corpuser:datahub", "type":"DATAOWNER"}]
            , "lastModified": {"time": #{ts}, "actor": "urn:li:corpuser:datahub"}
            }
          }
        , { "com.linkedin.pegasus2avro.dataset.DatasetProperties": {
              "description": {"string": #{schemaDescription}}
            }
          }
        , { "com.linkedin.pegasus2avro.schema.SchemaMetadata": {
              "schemaName": #{schemaName}
            , "platform": "urn:li:dataPlatform:"
            , "version": 0
            , "created": {"time": #{ts}, "actor": "urn:li:corpuser:datahub"}
            , "lastModified": {"time": #{ts}, "actor": "urn:li:corpuser:datahub"}
            , "hash": ""
            , "platformSchema": {
                "com.linkedin.pegasus2avro.schema.MySqlDDL": {
                  "documentSchema": "{}"
                , "tableSchema": "{}"
                }
              }
            , "fields": #{mkFields fields}
            }
          }
        ]
      }
    }
  }
  |]
  where
    urn :: String = printf "urn:li:dataset:(urn:li:dataPlatform:%s,%s,%s)"
                      platform schemaName ("PROD"::String)
    mkField (_:_:fieldPath:nativeDataType:description:[]) = [aesonQQ|
      {
        "fieldPath": #{fieldPath}
      , "description": {"string": #{description}}
      , "type": {"type": {"com.linkedin.pegasus2avro.schema.StringType": {}}}
      , "nativeDataType": #{nativeDataType}
      }
    |]
    mkFields = map mkField

main :: IO ()
main = do
  hSetEncoding stdout utf8
  let
    jvmArgs = case $(TH.lift =<< TH.runIO (lookupEnv "CLASSPATH")) of
      Nothing -> []
      Just cp -> [ cs ("-Djava.class.path=" ++ cp) ]
    platform :: T.Text =  "localhost_postgresql"
    -- dbUrl :: T.Text = "jdbc:mysql://localhost:3306/datahub?useSSL=false"
    -- dbUrl :: T.Text = "jdbc:oracle:thin@localhost:1521:EDWDB"
    dbUrl :: T.Text = "jdbc:postgresql://localhost:5432/datahub"
    
    dbUser :: T.Text  = "datahub"
    dbPassword :: T.Text = "datahub"
    
    -- dbDriver:: T.Text = "oracle.jdbc.OracleDriver" ;
    -- dbDriver:: T.Text = "com.mysql.jdbc.Driver" ;
    dbDriver:: T.Text = "org.postgresql.Driver" ;
    -- dbDriver:: T.Text = "com.microsoft.sqlserver.jdbc.SQLServerDriver" ;

    -- dbSQL :: T.Text = datasetMysqlSql
    -- dbSQL :: T.Text = datasetOracleSql
    dbSQL :: T.Text = datasetPostgresqlSql    
  runInBoundThread $ withJVM jvmArgs $ do
    [jDbUrl, jDbUser, jDbPassword, jDbDriver, jDbSQL ] <-
      mapM reflect [dbUrl, dbUser, dbPassword, dbDriver, dbSQL]
    
    result <- [java| {
      try {
        Class.forName($jDbDriver) ;
      } catch (ClassNotFoundException e) {
        e.printStackTrace() ;
        System.exit(1) ;
      }
      
      List<String[]> result = new ArrayList<String[]>() ;
      try (Connection con = DriverManager.getConnection($jDbUrl, $jDbUser, $jDbPassword) ;
           Statement st = con.createStatement(); ) {
        try (ResultSet rs = st.executeQuery($jDbSQL)) {
          while(rs.next()) {
            String[] row  = new String[] {
              Optional.ofNullable(rs.getString("schema_name")).orElse("")
            , Optional.ofNullable(rs.getString("schema_description")).orElse("")
            , Optional.ofNullable(rs.getString("field_path")).orElse("")
            , Optional.ofNullable(rs.getString("native_data_type")).orElse("")
            , Optional.ofNullable(rs.getString("description")).orElse("")
            } ;
            result.add(row) ;
          }
        }
        return result.toArray(new String[0][0]) ;
      } catch (SQLException e) {
        e.printStackTrace() ;
        return null ;
      }
    } |]
    
    rows :: [[T.Text]]  <- reify result
    runConduit $ C.yieldMany rows
              -- .| C.iterM (hPrint stderr)
              .| C.groupBy sameSchemaName
              -- .| C.iterM (hPrint stderr)
              .| C.map (mkMCE 0 platform) 
              .| C.mapM_ (J.encode >>> cs >>> putStrLn)
              .| C.sinkNull
    return ()
    where
      sameSchemaName (schemaNameL:_) (schemaNameR:_) = schemaNameL == schemaNameR
