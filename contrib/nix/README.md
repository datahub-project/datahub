# Nix sandbox for datahub


## Introduction
database is not suitable for virtualization for it's io performance.

so we use simple nix package tool to install package and setup service on physical machine.

we declare it, then it works. see [sandbox.nix] file for details.

it install software on /nix directory, and run service on launchpad(darwin) and systemd(linux).

NOTE: for linux, ensure 'systemd --user' process running.


## Roadmap

- [X] support mac and linux
- [ ] add environment check script
- [ ] add datahub nix package
- [ ] add datahub[gms, frontend, pipeline] service module
- [ ] add nixops distributed deploy


## Quickstart
1.  install nix and channel

```
  sudo install -d -m755 -o $(id -u) -g $(id -g) /nix
  curl https://nixos.org/nix/install | sh
  
  nix-channel --add https://nixos.org/channels/nixos-20.03 nixpkgs
  nix-channel --update nixpkgs
```

2. install home-manager

```
  nix-channel --add https://github.com/clojurians-org/home-manager/archive/v1.0.0.tar.gz home-manager
  nix-channel --update home-manager
  NIX_PATH=~/.nix-defexpr/channels nix-shell '<home-manager>' -A install
```

3. setup environment, and well done!
```
  NIX_PATH=~/.nix-defexpr/channels home-manager -f sandbox.nix switch
```

## Client connect
```
mysql                     => mysql -u root -S /nix/var/run/mysqld.sock
postgresql                => psql -h /nix/var/run postgres
elasticsearch             => curl http://localhost:9200
neo4j                     => cypher-shell -uneo4j -pneo4j
zookeeper                 => zkCli.sh
kafka                     => kafka-topics.sh --bootstrap-server localhost:9092 --list
confluent schema-registry => curl http://localhost:8081

```

## Environemnt Check

you only need install nix to run it!

```
nix-shell datahub-check.nix -A gms
```
