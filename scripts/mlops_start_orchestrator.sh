#!/bin/bash

set -euo pipefail

# echo "Fetching current GTO tags..."
# TAGS=$(git tag | grep "^data_client_one_" || true)


# if [ -z "$TAGS" ]; then
#   echo "No GTO tags found. Already clean."
# else
#   echo "Tags to delete:"
#   echo "$TAGS"
#   echo "$TAGS" | xargs git tag -d

#   echo "Deleting remote tags..."
#   echo "$TAGS" | xargs -I {} git push origin --delete {} 2>/dev/null || true

#   echo "Done. GTO registry reset to fresh start."
# fi

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