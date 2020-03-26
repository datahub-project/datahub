# datahub Ingestion Tool


## Introduction

some tool to ingestion [jdbc-database-schema] and [etl-lineage] metadata.

i split the ingestion procedure to two part: [datahub-producer] and different [metadata-generator]


## Roadmap

- [X] datahub-producer load json avro data.
- [ ] add dataset-jdbc generator
- [X] add lineage-hive generator
- [ ] add lineage-oracle generator
- [ ] enchance avro parser to show error information 


## Quickstart
1.  install nix and channel

```
  sudo install -d -m755 -o $(id -u) -g $(id -g) /nix
  curl https://nixos.org/nix/install | sh
  
  nix-channel --add https://nixos.org/channels/nixos-20.03 nixpkgs
  nix-channel --update nixpkgs
```

2. load json data to datahub

```
    cat sample/mce.json.dat | bin/datahub-producer.hs config
```

3. parse hive sql to  datahub
```
    ls sample/hive_*.sql | bin/lineage_hive_generator.hs | bin/datahub-producer.hs config
```
