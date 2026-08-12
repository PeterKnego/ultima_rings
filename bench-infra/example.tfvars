# Copy to terraform.tfvars (gitignored) and edit.
# Credentials are NOT here — set AWS_PROFILE, or point ENV_FILE at a file
# holding AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.

# 32 vCPU = 16 physical cores + SMT. This is exactly a 32-vCPU on-demand quota,
# so nothing else may be running. Use c7i.4xlarge (16 vCPU / 8 physical) if the
# apply is rejected; the sweep skips points larger than the host.
instance_type = "c7i.8xlarge"
region        = "us-east-1"

ssh_public_key       = "ssh-ed25519 AAAA... you@host"
ssh_private_key_file = "~/.ssh/id_ed25519"

# Your egress IP/32 — NOT 0.0.0.0/0. Check with:  curl https://checkip.amazonaws.com
allow_ssh_cidr = "203.0.113.4/32"

ttl_hours = 3              # advisory tag only; nothing auto-reaps
owner     = "urings-bench" # MUST differ from ultimadb-bench and uc-bench
