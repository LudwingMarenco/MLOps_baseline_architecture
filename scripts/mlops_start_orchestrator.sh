#!/bin/bash

set -euo pipefail

echo "Fetching current GTO tags..."
TAGS=$(git tag | grep "^data_client_one_" || true)


if [ -z "$TAGS" ]; then
  echo "No GTO tags found. Already clean."
else
  echo "Tags to delete:"
  echo "$TAGS"
  echo "$TAGS" | xargs git tag -d
  echo "Done. GTO registry reset to fresh start."
fi

dagster dev