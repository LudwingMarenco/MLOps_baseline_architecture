#!/bin/bash

set -euo pipefail

for dir in data/monitoring data/predictions models/; do
  if [ -d "$dir" ]; then
    count=$(find "$dir" -type f | wc -l)
    find "$dir" -type f -delete
    echo "Cleared $count file(s) from $dir"
  else
    echo "Directory $dir not found, skipping."
  fi
done

dagster dev