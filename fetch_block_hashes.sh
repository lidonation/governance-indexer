#!/bin/bash
set -e

# -------------------------------
# Configuration
# -------------------------------
START_EPOCH=507              # Conway hard fork epoch
OUTPUT_FILE="slot_hashes.csv"
BLOCKFROST_API="https://cardano-mainnet.blockfrost.io/api/v0"
BLOCKFROST_PROJECT_ID=""

# You MUST export your key first:
# export BLOCKFROST_PROJECT_ID=xxxx
if [ -z "$BLOCKFROST_PROJECT_ID" ]; then
  echo "❌ BLOCKFROST_PROJECT_ID not set"
  exit 1
fi

# -------------------------------
# Detect current epoch
# -------------------------------
echo "📊 Detecting current epoch..."

tip=$(curl -s \
  -H "project_id: $BLOCKFROST_PROJECT_ID" \
  "$BLOCKFROST_API/epochs/latest")

CURRENT_EPOCH=$(echo "$tip" | jq -r '.epoch // empty')

if [ -z "$CURRENT_EPOCH" ] || [ "$CURRENT_EPOCH" = "null" ]; then
  echo "❌ Failed to detect current epoch"
  exit 1
fi

echo "✅ Current epoch: $CURRENT_EPOCH"
echo ""

# -------------------------------
# Prepare output
# -------------------------------
> "$OUTPUT_FILE"
echo "slot,hash" >> "$OUTPUT_FILE"

success=0
fail=0

# -------------------------------
# Main loop
# -------------------------------
for ((epoch=START_EPOCH; epoch<=CURRENT_EPOCH; epoch++)); do
  echo "🔄 Epoch $epoch..."

  # 1️⃣ Get FIRST block hash of epoch (chain order)
  blocks=$(curl -s \
    -H "project_id: $BLOCKFROST_PROJECT_ID" \
    "$BLOCKFROST_API/epochs/${epoch}/blocks?count=1&order=asc")

  block_hash=$(echo "$blocks" | jq -r '.[0] // empty')

  if [ -z "$block_hash" ] || [ "$block_hash" = "null" ]; then
    echo "⚠️  No blocks found for epoch $epoch"
    ((fail++))
    sleep 0.3
    continue
  fi

  # 2️⃣ Resolve block to get slot
  block=$(curl -s \
    -H "project_id: $BLOCKFROST_PROJECT_ID" \
    "$BLOCKFROST_API/blocks/${block_hash}")

  slot=$(echo "$block" | jq -r '.slot // empty')

  if [ -z "$slot" ] || [ "$slot" = "null" ]; then
    echo "⚠️  Could not resolve slot for block $block_hash"
    ((fail++))
    sleep 0.3
    continue
  fi

  # 3️⃣ Write real chain point
  echo "${slot},${block_hash}" >> "$OUTPUT_FILE"
  ((success++))

  # Be kind to Blockfrost
  sleep 0.3
done

# -------------------------------
# Summary
# -------------------------------
echo ""
echo "✅ Done"
echo "   Success: $success epochs"
echo "   Failed:  $fail epochs"
echo "   Output:  $OUTPUT_FILE"
echo ""
echo "📄 Preview:"
head -5 "$OUTPUT_FILE"