with import <nixpkgs> {} ;
let
  queryparser_git = fetchFromGitHub {
      owner = "uber" ;
      repo = "queryparser" ;
      rev = "6015e8f273f4498326fec0315ac5580d7036f8a4" ;
      sha256 = "05pnifm5awyqxi6330v791b1cvw26xbcn2r20pqakvl8d3xyaxa4" ;
  } ; 
  haskellPackages = pkgs.haskellPackages.override {
    overrides = self: super: with pkgs.haskell.lib; {
      queryparser = appendConfigureFlag 
                      (dontHaddock (doJailbreak (self.callCabal2nix "queryparser" queryparser_git {})))
                      "--ghc-options=-XNoMonadFailDesugaring" ;
      queryparser-hive = dontHaddock (doJailbreak (self.callCabal2nix "queryparser-hive" (queryparser_git + /dialects/hive) {})) ;
    } ;
  };

in
mkShell {
  buildInputs = [
    (haskellPackages.ghcWithPackages ( p: 
      [ p.bytestring p.text p.string-conversions
        p.exceptions p.time
        p.aeson p.aeson-qq
        p.conduit        
        p.queryparser p.queryparser-hive
      ]
    ))
  ];
}

