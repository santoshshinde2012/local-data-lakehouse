#!/usr/bin/env bash
# graph_ask.sh -- start Claude Code limited to the lakehouse MCP servers: an allowlist launcher that fails closed.
#
#   scripts/graph_ask.sh                     interactive session in the repo root
#   scripts/graph_ask.sh "question"          one question, answer printed (claude -p)
#   scripts/graph_ask.sh --print-command     print the exact command line, start nothing
#   scripts/graph_ask.sh --check             verify the installed `claude` supports every flag, start nothing
#
# What the session gets (and nothing else):
#   --strict-mcp-config --mcp-config .mcp.json   only the lakehouse servers of this repo (graph, metrics,
#                                                lineage, cohorts); no user, project-scope or plugin servers
#   ENABLE_CLAUDEAI_MCP_SERVERS=false            no claude.ai connectors (mail, drive, calendar, trading ...)
#   --tools Read                                 the only built-in tool (no Bash, Edit, Write, WebFetch, WebSearch)
#   --restricted                                 no command-running tools or WebFetch, user / project / local
#                                                settings ignored, file tools confined to the repo
#   --permission-mode <default>                  the CLI's default mode (`manual` in Claude Code >= 2.1.28x,
#                                                `default` before); never bypassPermissions
#   --allowedTools mcp__lakehouse-*  Read        pre-approves the read-only lakehouse tools (needed for -p)
# The lakehouse servers themselves run under the macOS sandbox (scripts/graph_mcp.sh). Together this breaks the
# lethal trifecta at the client too: private data and untrusted text reach a session with no way out. An
# ordinary `claude` session in this repo keeps all of its tools and connectors and has no such guarantee.
#
# Fails closed (exit non-zero, nothing started) when `claude` is not on PATH, is older than MIN_VERSION, its
# --help lacks one of the flags, .mcp.json is missing, or an argument is not one of the forms above.
# Verify what a session really loads (Claude Code's own init event, no model call):
#   python scripts/check_graph_tools.py --profile <p> --skip-sweep --skip-smoke --ask-session [--ask-user-config]
# Works with /bin/bash 3.2; never installs or downloads anything.
set -euo pipefail

MIN_VERSION="2.1.248"
die() { printf 'graph_ask.sh: %s Nothing was started.\n' "$1" >&2; exit "${2:-1}"; }
realdir() { cd -P -- "$1" 2>/dev/null && REPLY="${PWD}"; }

SELF="${BASH_SOURCE[0]}"
while [ -L "${SELF}" ]; do
  LINK="$(readlink -- "${SELF}")"
  case "${LINK}" in
    /*) SELF="${LINK}" ;;
    *) case "${SELF}" in */*) SELF="${SELF%/*}/${LINK}" ;; *) SELF="${LINK}" ;; esac ;;
  esac
done
case "${SELF}" in */*) SELF_DIR="${SELF%/*}" ;; *) SELF_DIR="." ;; esac
realdir "${SELF_DIR}/.." || die "cannot resolve the repo root." 66
ROOT="${REPLY}"
MCP_CONFIG="${ROOT}/.mcp.json"

PRINT=0
CHECK=0
QUESTION=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --print-command) PRINT=1 ;;
    --check) CHECK=1 ;;
    -h|--help) sed -n '2,6p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    -*) die "unknown option '$1': only --print-command, --check or one quoted question are accepted." 64 ;;
    *) [ -z "${QUESTION}" ] || die "give at most one question (quote it)." 64; QUESTION="$1" ;;
  esac
  shift
done

CLAUDE="$(command -v claude || true)"   # empty when missing: handled on the next line
[ -n "${CLAUDE}" ] || die "the Claude Code CLI ('claude') is not on PATH. Install it (https://code.claude.com/docs/en/setup), sign in, then rerun." 69
[ -f "${MCP_CONFIG}" ] || die "missing ${MCP_CONFIG} (run from a checkout of this repo)." 66

# version gate: the flags below exist from MIN_VERSION on
RAW_VERSION="$("${CLAUDE}" --version 2>/dev/null || true)"   # parsed and checked below
HAVE_VERSION="$(printf '%s\n' "${RAW_VERSION}" | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' | head -n 1 || true)"   # no match: empty, refused next
[ -n "${HAVE_VERSION}" ] || die "could not read a version from 'claude --version'." 69
version_ge() {  # version_ge HAVE WANT: true when HAVE >= WANT (numeric, dot separated)
  local first
  first="$(printf '%s\n%s\n' "$2" "$1" | sort -t. -k1,1n -k2,2n -k3,3n | head -n 1)"
  [ "${first}" = "$2" ]
}
version_ge "${HAVE_VERSION}" "${MIN_VERSION}" || die "Claude Code ${HAVE_VERSION} is older than ${MIN_VERSION} (needs --strict-mcp-config, --tools and --restricted): run 'claude update'." 69

# flag support, read from the installed CLI's own help text
HELP="$("${CLAUDE}" --help 2>/dev/null || true)"   # checked flag by flag below
for flag in --strict-mcp-config --mcp-config --tools --restricted --permission-mode --allowedTools; do
  case "${HELP}" in *"${flag}"*) ;; *) die "this claude (${HAVE_VERSION}) does not document ${flag}; refusing to start without the allowlist." 69 ;; esac
done
MODES="${HELP#*--permission-mode}"   # the choices listed for --permission-mode only
MODES="${MODES%%)*}"
MODE=""
case "${MODES}" in
  *'"default"'*) MODE="default" ;;
  *'"manual"'*) MODE="manual" ;;
  *) die "cannot find the default permission mode (default or manual) among the --permission-mode choices." 69 ;;
esac

ARGS=(
  --strict-mcp-config --mcp-config "${MCP_CONFIG}"
  --tools Read
  --restricted
  --permission-mode "${MODE}"
  --allowedTools mcp__lakehouse-graph mcp__lakehouse-metrics mcp__lakehouse-lineage mcp__lakehouse-cohorts Read
)
[ -z "${QUESTION}" ] || ARGS+=(-p "${QUESTION}")

if [ "${PRINT}" = 1 ]; then
  printf 'cd %q && ENABLE_CLAUDEAI_MCP_SERVERS=false %q' "${ROOT}" "${CLAUDE}"
  printf ' %q' "${ARGS[@]}"
  printf '\n'
  exit 0
fi
if [ "${CHECK}" = 1 ]; then
  printf 'graph_ask.sh: ok: claude %s supports the allowlist (permission mode %s); servers from %s\n' \
    "${HAVE_VERSION}" "${MODE}" "${MCP_CONFIG}" >&2
  exit 0
fi

cd -- "${ROOT}"   # .mcp.json commands are relative to the repo root
ENABLE_CLAUDEAI_MCP_SERVERS=false exec "${CLAUDE}" "${ARGS[@]}"
