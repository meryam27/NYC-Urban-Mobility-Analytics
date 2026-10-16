terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
variable "region" {
  default = "eu-west-1"
}
variable "project" {
  default = "nyc-mobility"
}
// c'est cette ligne qui dit à terraform de se connecter a aws et de creer les ressources dans la region indiquée
provider "aws" {
  region = var.region
}
// recupere Account ID de l'utilisateur aws qui execute le code terraform
data "aws_caller_identity" "me" {}
// terraform va créer le backet et lake c'est le nom local que Terraform donne à cette ressource.
resource "aws_s3_bucket" "lake" {
  bucket        = "${var.project}-lake-${data.aws_caller_identity.me.account_id}"
  force_destroy = true
}
// on rend backet privé
resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true // si bucket a des acls publiques ignore les acls publiques
  restrict_public_buckets = true
}


// supprimer les fichiers générés par Athena dans bucket apres 7 jours
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    id     = "expire-athena-results"
    status = "Enabled"
    // ce filter qui permet de ne supprimer que les fichiers générés par athena et pas tout data lake
    filter {
      prefix = "athena-results/"
    }
    expiration {
      days = 7
    }
  }

}

// creation des bases de données AWS Glue Data catalog 
resource "aws_glue_catalog_database" "db" {
  for_each = toset(["raw", "staging", "marts"])
  name     = each.key
}

// cration du Workgroup athena 
resource "aws_athena_workgroup" "wg" {
  name          = var.project
  force_destroy = true
  configuration {
    //Les paramètres définis au niveau du Workgroup doivent être respectés par les requêtes.
    enforce_workgroup_configuration = true
    // Athena ne doit pas scanner plus de 5 Go de données par requete pour controler les couts
    bytes_scanned_cutoff_per_query = 5368709120
    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.bucket}/athena-results/"
    }
  }
}

// on veut que lorsqu'on écrit terraform output -raw bucket on obtient nom du bucket

output "bucket" {
  value = aws_s3_bucket.lake.bucket
}

