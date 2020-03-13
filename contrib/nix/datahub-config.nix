{
  services.linkedin-datahub-gms = {
    enable = true;
    sandbox = {
      jdbc.uri = "jdbc:postgresql://localhost:5432/datahub" ;
      jdbc.username = "datahub" ;
      jdbc.password = "datahub" ;
      elasticsearch.uris = [ "http://localhost:9200" ] ;
      neo4j.uri = "bolt://localhost:7687" ;
      neo4j.username = "neo4j" ;
      neo4j.password = "datahub" ;
      kafka.uris = [ "PLAINTEXT://localhost:9092" ] ;
      schema-registry.uris = [ "http://localhost:8081" ] ;
    } ;
    listener = "http://localhost:8080" ;
  } ;

  services.linkedin-datahub-frontend = {
    enable = true ;
    listener = "http://localhost:9001" ;
    linkedin-datahub-gms.uri = "http://localhost:8080" ;
  } ;
  services.linkedin-datahub-pipeline = {
    enable = true ;
    linkedin-datahub-gms.uri = "http://localhost:8080" ;
    sandbox = {
      kafka.uris = [ "PLAINTEXT://localhost:9092" ] ;
      schema-registry.uris = [ "http://localhost:8081" ] ;
    } ;
  } ;
}
