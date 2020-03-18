with import <nixpkgs> {} ;
let
in
mkShell {
  buildInputs = [
    (haskellPackages.ghcWithPackages ( p: 
      [ p.bytestring p.string-conversions
        p.exceptions 
        p.network-uri p.directory
        p.lens p.aeson p.lens-aeson p.avro p.hw-kafka-avro 
        p.hw-kafka-client 
        p.conduit p.hw-kafka-conduit
      ]
    ))
  ];
}
