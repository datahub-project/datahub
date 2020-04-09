#! /usr/bin/env nix-shell
#! nix-shell datahub-producer.hs.nix -i runghc

{-# LANGUAGE OverloadedStrings, FlexibleInstances, FlexibleContexts, ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

import System.Environment (getArgs)
import System.Directory (canonicalizePath)
import Data.Typeable (Typeable)
import Data.Functor ((<&>))
import Control.Arrow (left, right)
import Control.Monad ((>=>), when)
import Control.Monad.IO.Class (MonadIO(..))

import Control.Monad.Catch (Exception, MonadThrow(..))

import qualified Data.ByteString.Lazy as B
import qualified Data.ByteString.Lazy.Char8 as BC
import qualified Data.Text as T
import qualified Data.Aeson as J
import Data.String.Conversions (cs)
import qualified Data.Binary as BIN

import Data.HashMap.Strict  ((!))
import qualified Data.HashMap.Strict as MS
import qualified Data.Vector as V


import Control.Lens ((^?), (^..), folded, _Just)
import Data.Aeson.Lens (key, _Array, _String)

import qualified Data.Avro.Types as A (Value(..))
import qualified Data.Avro as A (Schema, Result(..))
import qualified Data.Avro.Schema as AS (
    Schema(..), resultToEither, buildTypeEnvironment
  , renderFullname, parseFullname, typeName, parseAvroJSON
  )
-- import Data.Avro.JSON (decodeAvroJSON)
import Data.Avro.Encode (encodeAvro)
import Data.Avro.Decode (decodeAvro)
-- import Data.Avro.Deriving (makeSchema)

import Kafka.Avro (
    SchemaRegistry(..), Subject(..), SchemaId(..)
  , schemaRegistry, sendSchema
  , extractSchemaId, loadSchema
  )

import Data.Conduit (ConduitT, ZipSink(..), getZipSink, runConduitRes, runConduit, bracketP, (.|), yield)
import qualified Data.Conduit.Combinators as C
import Kafka.Conduit.Sink (ProducerRecord(..), TopicName(..), ProducePartition(..), BrokerAddress(..), kafkaSink, brokersList)

import Network.URI (parseURI)
import Network.URI.Lens (uriAuthorityLens, uriRegNameLens, uriPortLens)

import System.Process (readProcess)

data StringException = StringException String deriving (Typeable, Show)
instance Exception StringException

decodeAvroJSON :: A.Schema -> J.Value -> A.Result (A.Value A.Schema)
decodeAvroJSON schema json =
  AS.parseAvroJSON union env schema json
  where
    env =
      AS.buildTypeEnvironment missing schema
    missing name =
      fail ("Type " <> show name <> " not in schema")

    union (AS.Union schemas) J.Null
      | AS.Null `elem` schemas =
          pure $ A.Union schemas AS.Null A.Null
      | otherwise                  =
          fail "Null not in union."
    union (AS.Union schemas) (J.Object obj)
      | null obj =
          fail "Invalid encoding of union: empty object ({})."
      | length obj > 1 =
          fail ("Invalid encoding of union: object with too many fields:" ++ show obj)
      | otherwise      =
          let
            canonicalize name
              | isBuiltIn name = name
              | otherwise      = AS.renderFullname $ AS.parseFullname name
            branch =
              head $ MS.keys obj
            names =
              MS.fromList [(AS.typeName t, t) | t <- V.toList schemas]
          in case MS.lookup (canonicalize branch) names of
            Just t  -> do
              nested <- AS.parseAvroJSON union env t (obj ! branch)
              return (A.Union schemas t nested)
            Nothing -> fail ("Type '" <> T.unpack branch <> "' not in union: " <> show schemas)
    union AS.Union{} _ =
      A.Error "Invalid JSON representation for union: has to be a JSON object with exactly one field."
    union _ _ =
      error "Impossible: function given non-union schema."

    isBuiltIn name = name `elem` [ "null", "boolean", "int", "long", "float"
                                 , "double", "bytes", "string", "array", "map" ]


fromRight :: (MonadThrow m, Show a) => String -> Either a b -> m b
fromRight label = either (throwM . StringException . (label ++) . show) return

fromJust :: (MonadThrow m, Show a) => String -> Maybe a -> m a
fromJust label = maybe (throwM . StringException $ (label ++ "value is missing") ) return

encodeJsonWithSchema :: (MonadIO m, MonadThrow m)
  => SchemaRegistry
  -> Subject
  -> A.Schema
  -> J.Value
  -> m B.ByteString
encodeJsonWithSchema sr subj schema json = do
  v <- fromRight "[decodeAvroJSON]" $  AS.resultToEither $ decodeAvroJSON schema json 
  mbSid <- fromRight "[SchemaRegistry.sendSchema]"=<< sendSchema sr subj schema
  return $ appendSchemaId  v mbSid
  where appendSchemaId v (SchemaId sid)= B.cons (toEnum 0) (BIN.encode sid) <> (encodeAvro v)

decodeJsonWithSchema :: (MonadIO m, MonadThrow m)
                 => SchemaRegistry
                 -> B.ByteString
                 -> m J.Value
decodeJsonWithSchema sr bs = do
  (sid, payload) <- maybe (throwM . StringException $ "BadPayloadNoSchemaId") return $ extractSchemaId bs
  schema <- fromRight "[SchemaRegistry.loadSchema]" =<< loadSchema sr sid
  J.toJSON <$> (fromRight "[Avro.decodeAvro]" $ decodeAvro schema payload)


parseNixJson :: FilePath -> IO J.Value
parseNixJson f = do
  stdout :: String <- read <$> readProcess "nix-instantiate" ["--eval", "--expr", "builtins.toJSON (import " ++ f ++ ")"] ""
  fromRight "[Aeson.eitherDecode] parse nix json" (J.eitherDecode (cs stdout))

main :: IO ()
main = do
  args <- getArgs
  when (length args /= 1) $
    error "  datahub-producer.hs [config-dir]"

  confDir <- canonicalizePath (head args)
  putStrLn ("confDir:" <> confDir)
  confJson <- parseNixJson (confDir <> "/" <> "datahub-config.nix")
  -- putStrLn ("confJson: " ++ show confJson)
  schema <- fromRight "[Aeson.eitherDecode] parse asvc file:" =<<
              J.eitherDecode <$> B.readFile (confDir <> "/" <> "MetadataChangeEvent.avsc")
  -- putStrLn ("schema: " ++ show schema)            

  let
    topic = "MetadataChangeEvent"
    -- schema = $(makeSchema "../MetadataChangeEvent.avsc")
    sandboxL = key "services".key "linkedin-datahub-pipeline".key "sandbox"
    urisL = key "uris". _Array.folded._String
    brokers = confJson ^.. sandboxL.key "kafka".urisL
    srs = confJson ^.. sandboxL.key "schema-registry".urisL
    brokers' = map (\uriText -> BrokerAddress . cs . concat $ parseURI (cs uriText)  ^.. _Just.uriAuthorityLens._Just.(uriRegNameLens <> uriPortLens)) brokers

  contents <- B.getContents <&> BC.lines
  sr <- schemaRegistry (cs (head srs))

  putStrLn " ==> beginning to send data..."
  runConduitRes $ C.yieldMany contents
               .| C.mapM (fromRight "[JSON.eitherDecode] read json record:".  J.eitherDecode)
               -- .| C.iterM (liftIO . putStrLn. cs . J.encode)
               .| C.mapM (encodeJsonWithSchema sr (Subject (topic <> "-value")) schema)
               -- .| C.iterM (decodeJsonWithSchema sr >=> liftIO . print . J.encode)
               .| C.map (mkRecord (TopicName topic))
               .| getZipSink ( ZipSink (kafkaSink (brokersList brokers')) *>
                               ZipSink ((C.length >>= yield) .| C.iterM (\n -> liftIO $ putStrLn ("total table num:" <> show n)) .| C.sinkNull))
  return ()               
  where
    mkRecord :: TopicName -> B.ByteString -> ProducerRecord
    mkRecord topic bs = ProducerRecord topic UnassignedPartition Nothing (Just (cs bs))
