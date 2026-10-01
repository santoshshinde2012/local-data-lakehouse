#!/usr/bin/env bash
# graph_mcp.sh -- start one lakehouse MCP server (stdio, read only). On macOS the server ALWAYS runs
# under sandbox-exec with config/graph/sandbox.sb and the launcher fails closed without it; on Linux
# there is no OS sandbox (one stderr banner); any other platform refuses.
#
#   scripts/graph_mcp.sh --toolset graph|metrics|lineage|cohorts|all|<comma list> [--build <build_dir>]
#                        [--max-chars N] [--call-timeout-s S] [--log-level L] [--verify sha256|rows]
#                        [--allow-unchecked] [--print-tools]
#   scripts/graph_mcp.sh --enable-cypher [--toolset cypher] [--build <build_dir>] [--max-chars N] ...
#                        guarded raw Cypher (graph_cypher, alone) over <build>/evidence.lbdb ONLY: on macOS
#                        under the same profile with no data tree readable except evidence.lbdb and
#                        evidence.json (the build's graph.lbdb, Parquet and manifest stay unreadable, so a
#                        statement past the guard still reads no label); refused with GRAPH_SANDBOX=0; on
#                        Linux refused unless GRAPH_ALLOW_UNSANDBOXED_CYPHER=1 (loud banner)
#   scripts/graph_mcp.sh --probe [--enable-cypher] [--build <build_dir>] [-- <probe args>]   # debug-only probe
#
# What it does, in order (nothing starts, and nothing is written, unless every check passes):
#   1. resolves the repo root from its own (symlink-resolved) location; works from any cwd
#   2. validates every argument against an allowlist (exit 64 on anything else)
#   3. picks the platform rule: macOS needs /usr/bin/sandbox-exec and the profile (exit 78 without
#      them, fail closed) unless GRAPH_SANDBOX=0; Linux runs without an OS sandbox; others refuse
#   4. resolves GRAPH_ROOT and the build ONCE to real paths (a later swap of $GRAPH_ROOT/current
#      cannot change what this server serves) and refuses a build outside GRAPH_ROOT. GRAPH_ROOT
#      becomes a readable tree inside the sandbox, so it must be a graph root (.lock or
#      <profile>/builds/, else exit 66) and never /, the account's home (from the user database, not
#      $HOME), the repo root or a parent of one of them (exit 64)
#   5. builds the server environment from an allowlist with `env -i` (secrets exported in the
#      parent shell, DYLD_* and loader overrides never reach Python); PYTHONPATH=src,
#      PYTHONDONTWRITEBYTECODE=1, PYTHONSAFEPATH=1, PYDANTIC_ERRORS_INCLUDE_URL=0
#   6. only now writes: $GRAPH_ROOT/logs, the audit key $GRAPH_ROOT/.audit_key (once), and the
#      pidfile <build>/.pids/<pid>.pid (+ <pid>.start, its start time) that keeps `gc` away (the
#      sandboxed server cannot write it), after sweeping the pidfiles of every build under GRAPH_ROOT
#      whose server is gone or whose pid was reused (the sandboxed server cannot unlink its own)
#   7. exec's `python -P -m lakehouse_graph.mcp_server` (directly, or under sandbox-exec), so the
#      pid, stdin EOF and signals reach Python
#
# Env in:  GRAPH_PY    interpreter (default <repo>/.venv-graph/bin/python; a bare name is looked up on PATH)
#          GRAPH_ROOT  graph root (default <repo>/data/graph)
#          GRAPH_SANDBOX=0                 macOS opt-out (prints a banner); the only way to run unsandboxed
#          GRAPH_LOG_LEVEL                 passed to the server (default WARNING)
#          GRAPH_SANDBOX_SIMULATE_MISSING=1  test hook; can only make the launcher stricter
#          GRAPH_ALLOW_UNSANDBOXED_CYPHER=1  Linux only: allow --enable-cypher without an OS sandbox (banner)
# Exit:    64 usage, 66 missing input, 69 interpreter unavailable, 78 no sandbox (fail closed),
#          65/71 from sandbox-exec itself (bad profile or parameter / exec failed); the server's own
#          codes (2 bad flags, 3 refused build, 130 signal) after the exec.
#
# Works with /bin/bash 3.2 (macOS). stdout is reserved for the MCP wire: every message goes to stderr.
# Never installs or downloads anything: a missing venv is an error that names `make graph-venv`.
set -euo pipefail

die() { printf 'graph_mcp.sh: %s (nothing was started)\n' "$1" >&2; exit "${2:-1}"; }
# realdir <dir>: physical path of an existing directory in REPLY (no subshell, no output).
realdir() { cd -P -- "$1" 2>/dev/null && REPLY="${PWD}"; }

START_DIR="${PWD}"
# Resolve this script through symlinks (bash 3.2: no readlink -f), so a linked launcher finds its repo.
SELF="${BASH_SOURCE[0]}"
while [ -L "${SELF}" ]; do
  LINK="$(readlink -- "${SELF}")"
  case "${LINK}" in
    /*) SELF="${LINK}" ;;
    *) case "${SELF}" in */*) SELF="${SELF%/*}/${LINK}" ;; *) SELF="${LINK}" ;; esac ;;
  esac
done
case "${SELF}" in */*) SELF_DIR="${SELF%/*}" ;; *) SELF_DIR="." ;; esac
realdir "${SELF_DIR}/.." || die "cannot resolve the repo root" 66
REPO="${REPLY}"
cd -- "${START_DIR}"

# --- arguments: an allowlist ----------------------------------------------------------------------
TOOLSET=""
BUILD=""
PROBE=0
PRINT_TOOLS=0
ENABLE_CYPHER=0
PY_FLAGS=()
EXTRA=()
need() { [ "$1" -ge 2 ] || die "$2 needs a value" 64; }
is_int() { case "$1" in ''|*[!0-9]*) return 1 ;; *) return 0 ;; esac; }
while [ "$#" -gt 0 ]; do
  case "$1" in
    --toolset) need "$#" "$1"; TOOLSET="$2"; shift 2 ;;
    --build) need "$#" "$1"; BUILD="$2"; shift 2 ;;
    --max-chars)
      need "$#" "$1"
      { is_int "$2" && [ "$2" -ge 4000 ] && [ "$2" -le 200000 ]; } || die "--max-chars must be a whole number from 4000 to 200000" 64
      PY_FLAGS+=(--max-chars "$2"); shift 2 ;;
    --call-timeout-s)
      need "$#" "$1"
      { is_int "$2" && [ "$2" -ge 1 ] && [ "$2" -le 120 ]; } || die "--call-timeout-s must be a whole number from 1 to 120" 64
      PY_FLAGS+=(--call-timeout-s "$2"); shift 2 ;;
    --log-level)
      need "$#" "$1"
      case "$2" in DEBUG|INFO|WARNING|ERROR) ;; *) die "--log-level must be DEBUG, INFO, WARNING or ERROR" 64 ;; esac
      PY_FLAGS+=(--log-level "$2"); shift 2 ;;
    --verify)
      need "$#" "$1"
      case "$2" in sha256|rows) ;; *) die "--verify must be sha256 or rows" 64 ;; esac
      PY_FLAGS+=(--verify "$2"); shift 2 ;;
    --allow-unchecked) PY_FLAGS+=(--allow-unchecked); shift ;;
    --print-tools) PRINT_TOOLS=1; PY_FLAGS+=(--print-tools); shift ;;
    --enable-cypher) ENABLE_CYPHER=1; PY_FLAGS+=(--enable-cypher); shift ;;
    --probe) PROBE=1; shift ;;
    --) shift; EXTRA=("$@"); break ;;
    *) die "unknown argument: $1 (see the usage at the top of scripts/graph_mcp.sh)" 64 ;;
  esac
done
if [ "${ENABLE_CYPHER}" = 1 ]; then
  # guarded raw Cypher is served alone: one tool over the evidence graph, never beside the template toolsets
  case "${TOOLSET}" in
    ''|cypher) TOOLSET="cypher" ;;
    *) die "--enable-cypher serves the cypher toolset alone: drop --toolset (or pass --toolset cypher)" 64 ;;
  esac
else
  [ -n "${TOOLSET}" ] || TOOLSET="graph"
  [ "${TOOLSET}" != "cypher" ] || die "the cypher toolset exists only with --enable-cypher (opt-in, sandbox only)" 64
  TOOLSET_RE='^(all|(graph|metrics|lineage|cohorts)(,(graph|metrics|lineage|cohorts))*)$'
  [[ "${TOOLSET}" =~ ${TOOLSET_RE} ]] || die "bad --toolset: use graph, metrics, lineage, cohorts, a comma list or all" 64
fi
[ "${PROBE}" = 1 ] || [ "${#EXTRA[@]}" -eq 0 ] || die "arguments after -- are only for --probe" 64

PLATFORM=""
case "${OSTYPE:-}" in
  darwin*) PLATFORM="darwin" ;;
  linux*) PLATFORM="linux" ;;
  *) die "unsupported platform '${OSTYPE:-unknown}': only macOS (sandboxed) and Linux are supported" 78 ;;
esac
if [ "${ENABLE_CYPHER}" = 1 ]; then
  if [ "${PLATFORM}" = "darwin" ] && [ "${GRAPH_SANDBOX:-1}" = "0" ]; then
    die "--enable-cypher runs only under the macOS sandbox (never with GRAPH_SANDBOX=0)" 64
  fi
  if [ "${PLATFORM}" = "linux" ] && [ "${GRAPH_ALLOW_UNSANDBOXED_CYPHER:-0}" != "1" ]; then
    die "--enable-cypher runs only under the macOS sandbox; Linux has no OS sandbox (override, at your own risk: GRAPH_ALLOW_UNSANDBOXED_CYPHER=1)" 64
  fi
fi

# --- the platform rule, decided before anything is written ----------------------------------------------
SANDBOX_EXEC="/usr/bin/sandbox-exec"      # absolute: never resolved through PATH
PROFILE="${REPO}/config/graph/sandbox.sb"
SANDBOXED=0
if [ "${PLATFORM}" = "darwin" ] && [ "${GRAPH_SANDBOX:-1}" != "0" ]; then
  if [ "${GRAPH_SANDBOX_SIMULATE_MISSING:-0}" = "1" ] || [ ! -x "${SANDBOX_EXEC}" ]; then
    die "sandbox-exec not available at ${SANDBOX_EXEC}; refusing to start unsandboxed (fail closed). Opt out explicitly with GRAPH_SANDBOX=0" 78
  fi
  [ -r "${PROFILE}" ] || die "sandbox profile missing: ${PROFILE} (fail closed)" 78
  SANDBOXED=1
fi

# Relative inputs are relative to the caller's cwd.
abspath() { case "$1" in /*) REPLY="$1" ;; *) REPLY="${START_DIR}/$1" ;; esac; }

# --- interpreter -------------------------------------------------------------------------------------
PY="${GRAPH_PY:-${REPO}/.venv-graph/bin/python}"
case "${PY}" in
  */*) abspath "${PY}"; PY="${REPLY}" ;;
  *) PY="$(command -v -- "${PY}" || true)"; [ -n "${PY}" ] || die "GRAPH_PY not found on PATH" 69 ;;  # empty: handled here
esac
[ -x "${PY}" ] || die "python not found: ${PY} (run: make graph-venv)" 69

# One interpreter start (outside the sandbox): where the runtime really lives (sys.executable is the path to
# exec, which keeps venv detection; the real paths feed the profile) and the account's home directory from
# the user database (not $HOME, which a caller can point anywhere).
PY_EXE=""; VENV_REAL=""; PY_BASE_REAL=""; PY_EXE_REAL=""; ACCOUNT_HOME=""
{ IFS= read -r PY_EXE && IFS= read -r VENV_REAL && IFS= read -r PY_BASE_REAL && IFS= read -r PY_EXE_REAL \
    && IFS= read -r ACCOUNT_HOME; } < <(
  "${PY}" -I -c 'import os, sys
print(sys.executable)
print(os.path.realpath(sys.prefix))
print(os.path.realpath(sys.base_prefix))
print(os.path.realpath(sys.executable))
try:
    import pwd
    print(os.path.realpath(pwd.getpwuid(os.getuid()).pw_dir))
except (ImportError, KeyError):  # no user-database entry (a container uid): the HOME variable stands in
    print("")') || die "cannot query the interpreter: ${PY}" 69
[ -n "${PY_EXE_REAL}" ] || die "cannot query the interpreter: ${PY}" 69

# --- paths (all real: Seatbelt matches on the resolved path) ------------------------------------------
SRC_REAL="${REPO}/src"
if realdir "${HOME:-/var/empty}"; then HOME_REAL="${REPLY}"; else HOME_REAL="/var/empty"; fi
[ -n "${ACCOUNT_HOME}" ] || ACCOUNT_HOME="${HOME_REAL}"
abspath "${GRAPH_ROOT:-${REPO}/data/graph}"
realdir "${REPLY}" || die "GRAPH_ROOT not found: ${REPLY} (build a graph first: make graph-local)" 66
GRAPH_ROOT_REAL="${REPLY}"
# GRAPH_ROOT is a readable tree inside the sandbox. Never /, the account's home, the repo root or a parent
# of one of them (that would open the whole home or the repo), and only a directory laid out as a graph
# root. (A $HOME pointed INSIDE the graph root, as graph_sandbox_check.py's probe B does to test the
# profile's explicit re-denies, opens nothing more than the graph root itself.)
covers() {  # covers A B: A is B, or a parent directory of B
  [ "$1" = "/" ] || [ "$1" = "$2" ] || case "$2" in "$1"/*) return 0 ;; *) return 1 ;; esac
}
for GUARDED in "${REPO}" "${ACCOUNT_HOME}"; do
  if covers "${GRAPH_ROOT_REAL}" "${GUARDED}"; then
    die "GRAPH_ROOT ${GRAPH_ROOT_REAL} is /, your home, the repo root or a parent of one of them: point it at a graph root such as ${REPO}/data/graph" 64
  fi
done
is_graph_root() {  # the layout scripts/build_graph_local.py makes: the build lock or <profile>/builds/
  local d
  [ -f "$1/.lock" ] && return 0
  for d in "$1"/*/builds; do
    [ -d "${d}" ] && return 0
  done
  return 1
}
is_graph_root "${GRAPH_ROOT_REAL}" || die "GRAPH_ROOT ${GRAPH_ROOT_REAL} is not a graph root (no .lock, no <profile>/builds/): build a graph first (make graph-local)" 66
if [ "${PRINT_TOOLS}" = 1 ]; then
  BUILD_REAL="${GRAPH_ROOT_REAL}"        # --print-tools reads no build; the profile still needs the parameter
else
  abspath "${BUILD:-${GRAPH_ROOT_REAL}/current}"
  realdir "${REPLY}" || die "build dir not found: ${REPLY} (make graph-local, or pass --build)" 66
  BUILD_REAL="${REPLY}"                  # resolved once: a later symlink swap cannot change it
  case "${BUILD_REAL}" in
    "${GRAPH_ROOT_REAL}"/*) ;;
    *) die "build ${BUILD_REAL} is not inside GRAPH_ROOT ${GRAPH_ROOT_REAL}" 64 ;;
  esac
  [ -f "${BUILD_REAL}/manifest.json" ] || die "no manifest.json in ${BUILD_REAL}: not a graph build" 66
  PY_FLAGS+=(--build "${BUILD_REAL}")
fi
cd -- "${START_DIR}"

# --- what the sandbox may read ---------------------------------------------------------------------------
# Template servers: src/, the graph root and the build. A cypher server: src/ and the two evidence files only
# (GRAPH_ROOT and BUILD_DIR are passed as src/, readable anyway), so graph.lbdb, the Parquet and manifest.json,
# everything that holds a label, stay unreadable even to a statement that gets past the Cypher guard.
EVIDENCE_DB_REAL="/dev/null"
EVIDENCE_META_REAL="/dev/null"
SB_GRAPH_ROOT="${GRAPH_ROOT_REAL}"
SB_BUILD_DIR="${BUILD_REAL}"
if [ "${ENABLE_CYPHER}" = 1 ]; then
  SB_GRAPH_ROOT="${SRC_REAL}"
  SB_BUILD_DIR="${SRC_REAL}"
  if [ "${PRINT_TOOLS}" = 0 ]; then
    for F in evidence.lbdb evidence.json; do
      [ -f "${BUILD_REAL}/${F}" ] && [ ! -L "${BUILD_REAL}/${F}" ] || die "no evidence graph in ${BUILD_REAL} (${F}): run scripts/build_evidence_graph.py --build ${BUILD_REAL}" 66
    done
    EVIDENCE_DB_REAL="${BUILD_REAL}/evidence.lbdb"
    EVIDENCE_META_REAL="${BUILD_REAL}/evidence.json"
  fi
  if [ "${PROBE}" = 1 ]; then   # the probe module lives in its own fixture dir (written by graph_sandbox_check.py)
    realdir "${GRAPH_ROOT_REAL}/.sandbox-check" || die "no probe fixtures in ${GRAPH_ROOT_REAL}/.sandbox-check" 66
    SB_GRAPH_ROOT="${REPLY}"
    cd -- "${START_DIR}"
  fi
fi

# --- writes: only now that every check has passed (all OUTSIDE the sandbox) ------------------------------
[ -d "${GRAPH_ROOT_REAL}/logs" ] || mkdir -p -- "${GRAPH_ROOT_REAL}/logs"
realdir "${GRAPH_ROOT_REAL}/logs" || die "cannot resolve the logs dir" 66
LOGS_REAL="${REPLY}"
cd -- "${START_DIR}"

# Per-install audit key (HMAC key for argument hashes), created once, atomically.
KEY_FILE="${GRAPH_ROOT_REAL}/.audit_key"
if [ ! -s "${KEY_FILE}" ]; then
  # best effort: without the file the server uses a per-process key (and says so in every audit line)
  (umask 077 && od -An -N32 -tx1 /dev/urandom | tr -d ' \n' > "${KEY_FILE}.$$" && ln "${KEY_FILE}.$$" "${KEY_FILE}") 2>/dev/null || true  # a lost race: the other launcher's key stands
  rm -f -- "${KEY_FILE}.$$"
fi

# Pidfile for `gc` (store.live_pids reads <build>/.pids/<pid>.pid). The exec chain below keeps this PID,
# but the sandboxed server can neither write nor remove the file, so every server that exits leaves its
# pidfile behind (Claude Code starts the four lakehouse servers at once and stops them together). Each
# launch therefore first sweeps the pidfiles of EVERY build under GRAPH_ROOT: one whose process is gone,
# or whose pid now belongs to another process (its start time differs from the one recorded beside it in
# <pid>.start), is removed. Between launches the pidfiles of the last session's servers remain; they hold
# nothing, since gc only counts live pids (and a pid reused before the next launch is caught by that
# launch's sweep).
pid_alive() {  # kill -0 also fails for another user's live process (EPERM): only "No such process" is gone
  local err
  err="$(export LC_ALL=C; kill -0 "$1" 2>&1)" && return 0
  case "${err}" in *"No such process"*) return 1 ;; *) return 0 ;; esac
}
PS_BIN="$(command -v ps 2>/dev/null || true)"   # no ps: start times are not compared (empty = unknown)
proc_start() {  # proc_start <pid>: the process start time as `ps -o lstart=` prints it, in REPLY ("" if unknown)
  REPLY=""
  [ -n "${PS_BIN}" ] || return 0
  REPLY="$(LC_ALL=C "${PS_BIN}" -o lstart= -p "$1" 2>/dev/null || true)"   # a gone pid prints nothing: unknown
}
sweep_pidfiles() {  # sweep_pidfiles <.pids dir>...: drop the pidfiles of servers that are gone or whose pid was reused
  local dir f pid want
  for dir in "$@"; do
    [ -d "${dir}" ] || continue
    for f in "${dir}"/*.pid; do
      [ -f "${f}" ] || continue
      pid="${f##*/}"
      pid="${pid%.pid}"
      is_int "${pid}" || continue
      if ! pid_alive "${pid}"; then
        rm -f -- "${f}" "${dir}/${pid}.start"
      elif [ -s "${dir}/${pid}.start" ]; then
        proc_start "${pid}"
        want=""
        IFS= read -r want < "${dir}/${pid}.start" || true   # no trailing newline: want still holds the line
        if [ -n "${REPLY}" ] && [ "${want}" != "${REPLY}" ]; then
          rm -f -- "${f}" "${dir}/${pid}.start"      # the pid now names another process
        fi
      fi
    done
    for f in "${dir}"/*.start "${dir}"/*.tmp; do     # a launcher that died between (or inside) its two writes
      [ -f "${f}" ] || continue
      pid="${f##*/}"
      pid="${pid%%.*}"
      if is_int "${pid}" && [ ! -e "${dir}/${pid}.pid" ] && ! pid_alive "${pid}"; then
        rm -f -- "${f}"
      fi
    done
  done
}
if [ "${PROBE}" = 0 ] && [ "${PRINT_TOOLS}" = 0 ] && mkdir -p -- "${BUILD_REAL}/.pids" 2>/dev/null; then
  PID_DIRS=("${BUILD_REAL}/.pids")
  for D in "${GRAPH_ROOT_REAL}"/*/builds/*/.pids; do
    if [ -d "${D}" ] && [ "${D}" != "${BUILD_REAL}/.pids" ]; then
      PID_DIRS+=("${D}")
    fi
  done
  sweep_pidfiles "${PID_DIRS[@]}"
  # best effort (gc keeps 3 builds anyway); each file is renamed into place, so a concurrent sweep never reads half
  proc_start "$$"
  if [ -n "${REPLY}" ]; then
    { printf '%s\n' "${REPLY}" > "${BUILD_REAL}/.pids/$$.start.tmp" \
        && mv -f -- "${BUILD_REAL}/.pids/$$.start.tmp" "${BUILD_REAL}/.pids/$$.start"; } 2>/dev/null || true  # best effort
  fi
  { printf '%s\n' "$$" > "${BUILD_REAL}/.pids/$$.pid.tmp" \
      && mv -f -- "${BUILD_REAL}/.pids/$$.pid.tmp" "${BUILD_REAL}/.pids/$$.pid"; } 2>/dev/null || true  # best effort: gc keeps 3
fi

if [ "${PROBE}" = 1 ]; then
  # The probe module is written into the graph root by scripts/graph_sandbox_check.py (the only
  # readable tree besides src/); it attacks the sandbox from inside and is never an MCP tool.
  PY_PATH="${SRC_REAL}:${GRAPH_ROOT_REAL}/.sandbox-check"
  PY_ARGS=(-P -m graph_sandbox_probe probe --build "${BUILD_REAL}" --logs "${LOGS_REAL}" --repo "${REPO}")
  if [ "${ENABLE_CYPHER}" = 1 ]; then PY_ARGS+=(--cypher); fi
else
  PY_PATH="${SRC_REAL}"
  PY_ARGS=(-P -m lakehouse_graph.mcp_server --toolset "${TOOLSET}" --logs "${LOGS_REAL}" ${PY_FLAGS[@]+"${PY_FLAGS[@]}"})
fi

# --- environment: an explicit allowlist ---------------------------------------------------------------
[ -n "${USER:-}" ] || USER="$(/usr/bin/id -un 2>/dev/null || printf 'nobody')"
[ -n "${LOGNAME:-}" ] || LOGNAME="${USER}"
CF_ENC="$(printf '0x%X:0x0:0x0' "${UID}")"
ENV_ARGS=(
  "PATH=/usr/bin:/bin"
  "HOME=${HOME_REAL}"
  "USER=${USER}"
  "LOGNAME=${LOGNAME}"
  "LANG=${LANG:-en_US.UTF-8}"
  "PYTHONPATH=${PY_PATH}"
  "PYTHONDONTWRITEBYTECODE=1"
  "PYTHONNOUSERSITE=1"
  "PYTHONSAFEPATH=1"
  "PYDANTIC_ERRORS_INCLUDE_URL=0"
  "GRAPH_ROOT=${GRAPH_ROOT_REAL}"
  "GRAPH_LOGS_DIR=${LOGS_REAL}"
  "GRAPH_REPO_ROOT=${REPO}"
  "GRAPH_LOG_LEVEL=${GRAPH_LOG_LEVEL:-WARNING}"
  "__CF_USER_TEXT_ENCODING=${__CF_USER_TEXT_ENCODING:-${CF_ENC}}"
)
# (__CF_USER_TEXT_ENCODING: CoreFoundation reads it at init; without it, it calls getpwuid(), an
#  opendirectoryd mach look-up and an /etc/passwd read that the sandbox denies.)
[ -z "${TZ:-}" ] || ENV_ARGS+=("TZ=${TZ}")

cd -- "${SRC_REAL}"   # a directory the sandbox may read, so os.getcwd() works

if [ "${PLATFORM}" = "linux" ]; then
  if [ "${ENABLE_CYPHER}" = 1 ]; then   # reached only with GRAPH_ALLOW_UNSANDBOXED_CYPHER=1 (checked above)
    printf '%s\n' \
      '################################################################################' \
      '# graph_mcp.sh: GRAPH_ALLOW_UNSANDBOXED_CYPHER=1 -- RAW CYPHER WITHOUT A SANDBOX #' \
      '# Linux has no OS sandbox: only the Cypher guard stands between a query and     #' \
      '# your files and network. Never expose this server to untrusted prompts.       #' \
      '################################################################################' >&2
    exec /usr/bin/env -i "${ENV_ARGS[@]}" "GRAPH_SANDBOXED=0" "GRAPH_ALLOW_UNSANDBOXED_CYPHER=1" \
      "${PY}" "${PY_ARGS[@]}" ${EXTRA[@]+"${EXTRA[@]}"}
  fi
  printf 'graph_mcp.sh: no OS sandbox on Linux; relying on template-only tools\n' >&2
  exec /usr/bin/env -i "${ENV_ARGS[@]}" "GRAPH_SANDBOXED=0" "${PY}" "${PY_ARGS[@]}" ${EXTRA[@]+"${EXTRA[@]}"}
fi

if [ "${SANDBOXED}" = 0 ]; then   # macOS with GRAPH_SANDBOX=0
  printf '%s\n' \
    '################################################################################' \
    '# graph_mcp.sh: GRAPH_SANDBOX=0 -- macOS sandbox DISABLED for this server.     #' \
    '# The server can read your files and reach the network. Do not use it with     #' \
    '# untrusted prompts.                                                           #' \
    '################################################################################' >&2
  exec /usr/bin/env -i "${ENV_ARGS[@]}" "GRAPH_SANDBOXED=0" "${PY}" "${PY_ARGS[@]}" ${EXTRA[@]+"${EXTRA[@]}"}
fi

exec /usr/bin/env -i "${ENV_ARGS[@]}" "GRAPH_SANDBOXED=1" \
  "${SANDBOX_EXEC}" -f "${PROFILE}" \
    -D "REPO_ROOT=${REPO}" \
    -D "SRC_DIR=${SRC_REAL}" \
    -D "VENV=${VENV_REAL}" \
    -D "PY_BASE=${PY_BASE_REAL}" \
    -D "PY_EXE=${PY_EXE_REAL}" \
    -D "GRAPH_ROOT=${SB_GRAPH_ROOT}" \
    -D "BUILD_DIR=${SB_BUILD_DIR}" \
    -D "LOGS_DIR=${LOGS_REAL}" \
    -D "HOME_DIR=${HOME_REAL}" \
    -D "EVIDENCE_DB=${EVIDENCE_DB_REAL}" \
    -D "EVIDENCE_META=${EVIDENCE_META_REAL}" \
    "${PY_EXE}" "${PY_ARGS[@]}" ${EXTRA[@]+"${EXTRA[@]}"}
