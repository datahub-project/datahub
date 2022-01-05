resource "aws_glue_registry" "dataportal-kafka-schema-registry" {
  registry_name = "dataportal-schema-registry"
  tags = {
    Service = "dataportal"
  }
}
