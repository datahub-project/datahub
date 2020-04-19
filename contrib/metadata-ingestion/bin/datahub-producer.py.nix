with import <nixpkgs> {} ;
mkShell {
  buildInputs = [
    python38
  ] ++ (with python38Packages ;[
    confluent-kafka
    requests
    # avro-python3
    avro3k
  ]) ;
}
