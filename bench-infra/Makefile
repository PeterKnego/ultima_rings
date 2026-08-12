# bench-infra — CPU-topology rig for ultima_rings
#
# Provisions ONE high-core-count AWS host, rsyncs this working tree, and sweeps
# the wait-strategy benchmarks across pinned core counts. Storage-agnostic:
# unlike ultima_db's rig this wants cores, not NVMe.
#
# Credentials are never stored here. Point ENV_FILE at a gitignored file with
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (or set AWS_PROFILE).
#
# ALWAYS `make destroy`. Nothing auto-reaps; ttl_hours is only a tag.

SHELL := /bin/bash
TF     := terraform -chdir=terraform
TFVARS := terraform.tfvars
VARF   := -var-file=../$(TFVARS)
ENV_FILE ?= .env
REMOTE_HOME := /home/ubuntu
SRC := $(shell cd .. && pwd)

ifneq ($(wildcard $(ENV_FILE)),)
include $(ENV_FILE)
export
endif

SSH_KEY  := $(shell grep -E '^ssh_private_key_file' terraform.tfvars 2>/dev/null | cut -d'"' -f2)
SSH_OPTS := -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15
HOST      = $(shell $(TF) output -json nodes 2>/dev/null | jq -r '.[0].public_ip // empty')
SSH       = ssh $(SSH_OPTS) -i $(SSH_KEY)

.PHONY: help init up provision sweep fetch destroy status ssh
.DEFAULT_GOAL := help

help: ## show targets
	@grep -hE '^[a-z][a-z0-9/_-]*:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/ -- /' | expand -t22

init: ## terraform init
	$(TF) init

up: ## create the host, then install toolchain + tune OS
	$(TF) apply -auto-approve $(VARF)
	@echo "waiting for sshd..."
	@for i in $$(seq 1 60); do $(SSH) ubuntu@$(HOST) true 2>/dev/null && break || sleep 5; done
	$(MAKE) provision

provision: ## install toolchain + tune OS on the host
	scp $(SSH_OPTS) -i $(SSH_KEY) remote/provision.sh ubuntu@$(HOST):$(REMOTE_HOME)/
	$(SSH) ubuntu@$(HOST) 'bash $(REMOTE_HOME)/provision.sh'

sweep: ## rsync the tree and run the core-count sweep, then fetch
	rsync -az --delete -e '$(SSH)' \
	  --exclude target --exclude .git --exclude bench-infra \
	  $(SRC)/ ubuntu@$(HOST):$(REMOTE_HOME)/ultima_rings/
	scp $(SSH_OPTS) -i $(SSH_KEY) remote/sweep.sh ubuntu@$(HOST):$(REMOTE_HOME)/
	$(SSH) ubuntu@$(HOST) 'source ~/.cargo/env && SKIP_BAKEOFF=$(SKIP_BAKEOFF) bash $(REMOTE_HOME)/sweep.sh'
	$(MAKE) fetch

fetch: ## pull results into bench-out/<timestamp>/
	@ts=$$(date -u +%Y%m%dT%H%M%SZ); mkdir -p bench-out/$$ts; \
	rsync -az -e '$(SSH)' ubuntu@$(HOST):$(REMOTE_HOME)/bench-out/ bench-out/$$ts/; \
	echo "pulled to bench-out/$$ts"; ls -la bench-out/$$ts

status: ## show the instance + uptime (cost guard)
	@$(TF) output -json nodes 2>/dev/null | jq -r '.[] | "\(.name)  \(.public_ip)"' || echo "nothing up"
	@$(SSH) ubuntu@$(HOST) uptime 2>/dev/null || true

ssh: ## ssh to the host
	$(SSH) ubuntu@$(HOST)

destroy: ## TEAR DOWN — always run this
	$(TF) destroy -auto-approve $(VARF)
