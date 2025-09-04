#!/usr/bin/env bash
set -euo pipefail
python -m formiq.cli daily --config examples/formiq.yml --reporter markdown
