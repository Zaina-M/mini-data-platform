#!/usr/bin/env bash
set -euo pipefail

sqlfluff lint sql/ --dialect postgres
