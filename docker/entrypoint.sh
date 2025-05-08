#!/usr/bin/bash
set -eoux pipefail

# Migrate all databases
CS_ENV=all alembic upgrade head

# Run all server processes (except mysql)
/usr/bin/supervisord
