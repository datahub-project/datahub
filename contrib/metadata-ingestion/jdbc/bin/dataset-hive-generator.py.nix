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

  sasl = python3Packages.buildPythonPackage rec {
      pname = "sasl" ;
      version = "0.2.1" ;

      src = python3Packages.fetchPypi {
        inherit pname version ;
        sha256 = "04f22e17bbebe0cd42471757a48c2c07126773c38741b1dad8d9fe724c16289d" ;
      } ;
      doCheck = false;
      propagatedBuildInputs = [ cyrus_sasl ] ++ (with python3Packages ; [six]) ;
  } ;

  thrift_sasl = python3Packages.buildPythonPackage rec {
      pname = "thrift_sasl" ;
      version = "0.4.2" ;

      src = python3Packages.fetchPypi {
        inherit pname version ;
        sha256 = "6a1c54731cb3ce61bdc041d9dc36e21f0f56db0163bb7b57be84de3fda70922f" ;
      } ;
      doCheck = false;
      propagatedBuildInputs = with python3Packages; [ thrift sasl ] ;
  } ;

  PyHive = python3Packages.buildPythonPackage rec {
      pname = "PyHive" ;
      version = "0.6.1" ;

      src = python3Packages.fetchPypi {
        inherit pname version ;
        sha256 = "a5f2b2f8bcd85a8cd80ab64ff8fbfe1c09515d266650a56f789a8d89ad66d7f4" ;
      } ;
      doCheck = false;
      propagatedBuildInputs = with python3Packages ; [ dateutil future thrift sasl thrift_sasl ];
  } ;

in
mkShell {
  buildInputs = (with python3Packages ;[
    python
    requests
    PyHive

    simplejson
    # avro-python3-1_8
    # confluent-kafka
  ]) ;
}
