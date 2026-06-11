#!/usr/bin/env bash
# Run ado-universal-pr-review batch on all Active Traffix Medallion PRs.
# Schedule with cron, e.g. weekdays 8:00am local:
#   0 8 * * 1-5 $HOME/.cursor/skills/ado-universal-pr-review/scripts/run-daily-ado-pr-review-batch.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROMPT_FILE="$SKILL_ROOT/prompts/daily-batch.prompt.txt"
LOG_DIR="${ADO_PR_REVIEW_LOG_DIR:-$HOME/.cursor/logs/ado-pr-review-batch}"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="$LOG_DIR/run-$TIMESTAMP.log"

mkdir -p "$LOG_DIR"

if [[ ! -f "$PROMPT_FILE" ]]; then
  echo "Missing prompt file: $PROMPT_FILE" >&2
  exit 1
fi

if command -v agent >/dev/null 2>&1; then
  AGENT_BIN="$(command -v agent)"
elif [[ -x "$HOME/.cursor/bin/agent" ]]; then
  AGENT_BIN="$HOME/.cursor/bin/agent"
else
  echo "Cursor agent CLI not found. Install: curl https://cursor.com/install -fsS | bash" >&2
  exit 1
fi

PROMPT="$(tr -d '\r' < "$PROMPT_FILE")"

{
  echo "=== ADO daily PR review batch ==="
  echo "Started: $(date -Is)"
  echo "Agent: $AGENT_BIN"
  echo "Prompt: $PROMPT_FILE"
  echo
  "$AGENT_BIN" -p "$PROMPT"
  echo
  echo "Finished: $(date -Is)"
} 2>&1 | tee "$LOG_FILE"

echo "Log: $LOG_FILE"
