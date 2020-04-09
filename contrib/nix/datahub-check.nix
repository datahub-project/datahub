{ pkgs ? import <nixpkgs> {} }:

with pkgs ;
let
  datahub = import ./datahub-config.nix ;
  build-prompt = ''
    echo This derivation is not buildable, instead run it using nix-shell.
    exit 1
  '' ;
  parse-uri = uri : 
    let
      uriSchemaSplit = builtins.split "://" uri ;
      schema = builtins.head uriSchemaSplit ;
      uriNoSchema = lib.last uriSchemaSplit ;

      uriPathSplit = builtins.split "/" uriNoSchema ;
      hostPort = builtins.head uriPathSplit ;
      path = lib.optionalString (builtins.length uriPathSplit > 1) (lib.last uriPathSplit) ;
      
      hostPortSplit = builtins.split ":" hostPort ;
      host = builtins.head hostPortSplit ;
      port = lib.last hostPortSplit ;

    in { inherit schema host port path ; } ;
  gms = 
    let 
      gms-conf = datahub.services.linkedin-datahub-gms ;
      jdbc-uri = parse-uri gms-conf.sandbox.jdbc.uri ;
      elasticsearch-uri = parse-uri (builtins.head gms-conf.sandbox.elasticsearch.uris) ;
      neo4j-uri = parse-uri gms-conf.sandbox.neo4j.uri ;
      kafka-uri = parse-uri (builtins.head gms-conf.sandbox.kafka.uris) ;
      schema-registry-uri = parse-uri (builtins.head gms-conf.sandbox.schema-registry.uris) ;
      gms-uri = parse-uri gms-conf.listener ;

      check-port = name : uri : ''
        echo "  [${name}] checking port..."
        ${netcat-gnu}/bin/nc -z ${uri.host} ${uri.port}
        if [ $? != 0 ]; then echo "  [${name}] !ERROR: can not connec to ${uri.host}:${uri.port}" && exit 1; fi
      '' ;
      check-jdbc-user = ''
        # echo "  [jdbc] checking username and password..." 
      '' ;
      check-jdbc-table = ''
        # echo "  [jdbc] checking  [metadata_aspect] table..."
      '' ;
      check-elasticsearch-index = ''
        # echo "  [elasticsearch] checking [corpuserinfodocument, datasetdocument] indices ..."
      '' ;
      check-neo4j-user = ''
        # echo "  [neo4j] checking user and password..."      
      '' ;
      check-kafka-topic = ''
        # echo "  [kafka] checking [MetadataChangeEvent, MetadataAuditEvent] indices..."
      '' ;
    in 
      stdenv.mkDerivation {
        name = "gms-check" ;

        buildInputs = [ netcat-gnu ] ;

        preferLocalBuild = true ;
        buildCommand = build-prompt ;
    
        shellHookOnly = true;
        shellHook = ''
          echo "******** checking sandbox.jdbc "
          ${check-port "jdbc" jdbc-uri}
          ${check-jdbc-user }
          ${check-jdbc-table }
          
          echo "******** checking sandbox.elasticsearch "
          ${check-port "elasticsearch" elasticsearch-uri}
          ${check-elasticsearch-index}

          echo "******** checking sandbox.neo4j "
          ${check-port "neo4j" neo4j-uri}
          ${check-neo4j-user }

          echo "******** checking sandbox.kafka "
          ${check-port "kafka" kafka-uri}
          ${check-kafka-topic }

          echo "******** checking sandbox.schema-registry "
          ${check-port "schema-registry" schema-registry-uri}

          echo "******** checking gms "
          ${check-port "gms" gms-uri}
          exit 0
        '' ;
      } ;
  frontend = 
    let
      frontend-conf = ddatahub.services.linkedin-datahub-frontend ;
    in {} ;
  pipeline = 
    let
      pipeline-conf = ddatahub.services.linkedin-datahub-pipeline ;
    in {} ;
in { inherit gms frontend pipeline;}
