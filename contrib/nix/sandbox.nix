{ config, pkgs, ... }:

{
  # Let Home Manager install and manage itself.
  programs.home-manager.enable = true;

  # This value determines the Home Manager release that your
  # configuration is compatible with. This helps avoid breakage
  # when a new Home Manager release introduces backwards
  # incompatible changes.
  #
  # You can update Home Manager without changing this value. See
  # the Home Manager release notes for a list of state version
  # changes in each release.
  home.stateVersion = "19.09";

  
  environment.systemPackages = [
    pkgs.gradle

    pkgs.postgresql_11
    pkgs.mysql57
    pkgs.elasticsearch
    pkgs.neo4j
    pkgs.zookeeper
    pkgs.apacheKafka
    pkgs.confluent-platform
    pkgs.kafkacat
    pkgs.neo4j
  ];

  services.postgresql = { 
    enable = true ; 
    package = pkgs.postgresql_11 ;
    dataDir = "/opt/nix-module/data/postgresql" ;
  } ;

  services.mysql = {
    enable = true ;
    # package = pkgs.mysql80 ;
    package = pkgs.mysql57 ;
    dataDir = "/opt/nix-module/data/mysql" ;
  } ;

  services.elasticsearch = {
    enable = true ;
    # package = pkgs.elasticsearch7 ;
    package = pkgs.elasticsearch ;
    dataDir = "/opt/nix-module/data/elasticsearch" ;
  } ;
  
  services.neo4j = {
    enable = true ;
    package = pkgs.neo4j ;
    directories.home = "/opt/nix-module/data/neo4j" ;
  } ;

  services.zookeeper = {
    enable = true ;
    package = pkgs.zookeeper ;
    dataDir = "/opt/nix-module/data/zookeeper" ;
  } ;

  services.apache-kafka = {
    enable = true ;
    package = pkgs.apacheKafka ;
    logDirs = [ "/opt/nix-module/data/kafka" ] ;
    zookeeper = "localhost:2181" ;
    extraProperties = ''
      offsets.topic.replication.factor = 1
      zookeeper.session.timeout.ms = 600000
    '' ;    
  } ;

  services.confluent-schema-registry = {
    enable = true ;
    package = pkgs.confluent-platform ;
    kafkas = [ "PLAINTEXT://localhost:9092" ] ;
  } ;
}
