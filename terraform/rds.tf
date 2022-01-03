data "aws_ssm_parameter" "private_subnet_ids" {
  name = "/network/private_subnets"
}

data "aws_security_group" "local" {
  #TODO: Update for prod if needed
  name = local.security_group[terraform.workspace]
}

resource "aws_db_subnet_group" "dataportal" {
  name       = "dataportal"
  subnet_ids = split(",", data.aws_ssm_parameter.private_subnet_ids.value)
}

resource "aws_db_instance" "dataportal" {
  identifier             = "dataportal"
  allocated_storage      = 200
  storage_type           = "gp2"
  engine                 = "mysql"
  engine_version         = "5.7"
  instance_class         = "db.t2.small"
  name                   = "dataportal"
  username               = "dataportal"
  password               = "Passw0rd"
  port                   = 3306
  publicly_accessible    = false
  vpc_security_group_ids = [data.aws_security_group.local.id]
  db_subnet_group_name   = aws_db_subnet_group.dataportal.name
  storage_encrypted      = true
  kms_key_id             = data.aws_ssm_parameter.red-key-arn.value
  tags = {
    Service = "dataportal"
  }

  backup_retention_period = 7
}
