variable "instance_type" {
  description = <<-EOT
    EC2 instance type. This rig varies CPU topology, not storage, so it wants
    many *physical* cores and no NVMe. c7i.8xlarge is 32 vCPU = 16 physical
    cores + SMT, which is exactly the account's 32 on-demand vCPU limit — any
    other running instance will make it fail. Fall back to c7i.4xlarge
    (16 vCPU / 8 physical) if quota bites.
  EOT
  type        = string
  default     = "c7i.8xlarge"
}

variable "region" {
  description = "AWS region."
  type        = string
  default     = "us-east-1"
}

variable "ssh_public_key" {
  description = "SSH public key contents to install on the host."
  type        = string
}

variable "ssh_private_key_file" {
  description = "Path to the matching private key, written into the Ansible inventory by the Makefile."
  type        = string
}

variable "allow_ssh_cidr" {
  description = "CIDR allowed to SSH (e.g. your IP/32). Do NOT use 0.0.0.0/0."
  type        = string
}

variable "ttl_hours" {
  description = "Advisory TTL tag for the cost guard. Nothing auto-reaps."
  type        = number
  default     = 4
}

variable "owner" {
  description = "Owner tag prefix. MUST differ from ultima_db's (ultimadb-bench) and ultima_cluster's (uc-bench) — every resource name derives from it."
  type        = string
  default     = "urings-bench"
}
