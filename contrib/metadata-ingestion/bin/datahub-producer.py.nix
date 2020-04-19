with import <nixpkgs> {} ;
let
  avro-python3-1_8 = python3Packages.buildPythonPackage rec {
      pname = "avro-python3" ;
      version = "1.8.2" ;

      src = python3Packages.fetchPypi {
        inherit pname version ;
        sha256 = "f82cf0d66189600b1e6b442f650ad5aca6c189576723dcbf6f9ce096eab81bd6" ;
      } ;
      doCheck = false;
  } ;
  avro-python3-1_7 = python3Packages.buildPythonPackage rec {
      pname = "avro-python3" ;
      version = "1.7.7" ;

      src = python3Packages.fetchPypi {
        inherit pname version ;
        sha256 = "b0a738e7a2b4bbe63029262fa1cd7c08a572341d267d543a08853e526356c1ea" ;
      } ;
      doCheck = false;
  } ;

in
mkShell {
  buildInputs = (with python3Packages ;[
    python
    confluent-kafka
    requests
    avro-python3-1_8
  ]) ;
}
