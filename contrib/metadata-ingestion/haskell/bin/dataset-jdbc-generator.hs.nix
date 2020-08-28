with import <nixpkgs> {} ;
let
  inline_java_git = fetchFromGitHub {
      owner = "tweag" ;
      repo = "inline-java" ;
      rev = "a897d32df99e4ed19314d2a7e245785152e9099d" ;
      sha256 = "00pk19j9g0mm9sknj3aklz01zv1dy234s3vnzg6daq1dmwd4hb68" ;
  } ; 
  haskellPackages = pkgs.haskellPackages.override {
    overrides = self: super: with pkgs.haskell.lib; {
      jni = overrideCabal (self.callCabal2nix "jni" (inline_java_git + /jni) {}) (drv: {
        preConfigure = ''
          local libdir=( "${pkgs.jdk}/lib/openjdk/jre/lib/"*"/server" )
          configureFlags+=" --extra-lib-dir=''${libdir[0]}"
        '' ;
      }) ;

      jvm = overrideCabal (self.callCabal2nix "jvm" (inline_java_git + /jvm) {}) (drv: {
        doCheck = false ;
      }) ;
      inline-java = overrideCabal (self.callCabal2nix "inline-java" inline_java_git {}) (drv: {
        doCheck = false ;
      }) ;
      jvm-batching = overrideCabal (self.callCabal2nix "jvm-batching" (inline_java_git + /jvm-batching) {}) (drv: {
        doCheck = false ;
      }) ;
      jvm-streaming = overrideCabal (self.callCabal2nix "jvm-streaming" (inline_java_git + /jvm-streaming) {}) (drv: {
        doCheck = false ;
      }) ;

    } ;
  };

in
mkShell {
  buildInputs = [
    pkgs.jdk
    pkgs.postgresql_jdbc
    pkgs.mysql_jdbc
    pkgs.mssql_jdbc
    pkgs.oracle-instantclient

    (haskellPackages.ghcWithPackages ( p: 
      [ p.bytestring p.string-conversions
        p.interpolatedstring-perl6
        p.aeson p.aeson-qq
        p.exceptions 
        p.inline-java

        p.conduit        
      ]
    ))
  ];
}
