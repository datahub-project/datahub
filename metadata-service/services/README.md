# Service Layer

Module to abstract away business logic from implementation specific libraries to make them lighter weight from a 
dependency perspective. Service classes should be here unless they require direct usage of implementation specific libraries
(i.e. ElasticSearch, Ebean, Neo4J, etc.).