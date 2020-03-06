# Nix sandbox for datahub


## Introduction
database is not suitable for virtualization for it's io performance.

so we use simple nix package tool to install package and setup service on physical machine.

we declare it, then it works. see [sandbox.nix] file for details.

it install software on /nix directory, and run service on launchpad(darwin) and systemd(linux).


## Roadmap
currently i already run all service on mac os. 
linux os will be tested and supportd will come sooner.
  

## Quickstart
1.  install nix and channel

```
  sudo install -d -m755 -o $(id -u) -g $(id -g) /nix
  curl https://nixos.org/nix/install | sh
  
  nix-channel --add https://nixos.org/channels/nixos-20.03 nixos-20.03
  nix-channel --update nixos-20.03
```

2. install home-manager

```
  nix-channel --add https://github.com/clojurians-org/home-manager/archive/master.tar.gz home-manager
  nix-channel --update
  nix-shell '<home-manager>' -A install
```

3. setup environment, and well done!
```
  export NIX_PATH=~/.nix-defexpr/channels
  home-manager -I home-manager=<home-manager> -f sandbox.nix switch
```
