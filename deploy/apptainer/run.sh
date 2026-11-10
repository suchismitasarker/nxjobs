#!/usr/bin/env bash
#
# Run the NxReduce Job Submission app from its Apptainer image.
#
# This is a convenience wrapper so you don't have to remember the --bind line.
# It binds the shared filesystem into the container and starts the web app on
# port 5050.
#
# USAGE
#   cd nxsjobs-submission
#   deploy/apptainer/run.sh                    # uses deploy/apptainer/nxjobs.sif
#   deploy/apptainer/run.sh /path/to/nxjobs.sif
#
# OVERRIDE BEAMTIME / LOGO (optional):
#   NXJOBS_ROOT=/nfs/chess/id4baux/2026-3 \
#   NXJOBS_CHESS_LOGO=/nfs/chess/id4baux/chesslogo.png \
#   deploy/apptainer/run.sh
#
# --------------------------------------------------------------------------
# SUBMIT JOBS FROM INSIDE THE CONTAINER (SSH)  -- works for ANY CLASSE user
# --------------------------------------------------------------------------
# The container itself has no SGE/qsub and no NFS group-write permission, so to
# actually submit jobs it logs in over SSH to the cluster AS YOU and runs qsub
# there. Turn it on with NXJOBS_SSH_ENABLE=1.
#
# By default it submits as whoever is running this script ($USER) to
# lnx201.classe.cornell.edu — so every person who runs the container submits
# under their OWN CLASSE account. No editing required per user.
#
#   NXJOBS_SSH_ENABLE=1 deploy/apptainer/run.sh
#
# Override any of these if your login differs:
#   NXJOBS_SSH_ENABLE=1 \
#   NXJOBS_SSH_HOST=lnx201.classe.cornell.edu \   # cluster submit host
#   NXJOBS_SSH_USER=$USER \                        # your CLASSE ID (auto)
#   NXJOBS_SSH_KEY=$HOME/.ssh/id_ed25519 \         # your private key (auto if present)
#   NXJOBS_SSH_PORT=22 \
#   NXJOBS_NXSETUP=/nfs/chess/id4baux/nxserver/nxsetup.sh \
#   deploy/apptainer/run.sh
#
# ONE-TIME KEY SETUP (each user does this once, on the machine they run from):
#   ssh-keygen -t ed25519                              # if you don't have a key
#   ssh-copy-id <yourid>@lnx201.classe.cornell.edu     # one Duo login to install it
# After that, submissions log in with no password and no Duo prompt.
#
# TIP: to keep it running after you log out, start it inside tmux or screen:
#   tmux new -s nxjobs 'NXJOBS_SSH_ENABLE=1 deploy/apptainer/run.sh'   (detach: Ctrl-b then d)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"   # .../deploy/apptainer
SIF="${1:-${SCRIPT_DIR}/nxjobs.sif}"

# The shared filesystem the app reads scans from and writes scripts to.
# Override BIND if your site mounts it somewhere else.
BIND="${NXJOBS_BIND:-/nfs/chess:/nfs/chess}"

# Defaults match the app; export only if set so we don't clobber the image's.
NXJOBS_ROOT="${NXJOBS_ROOT:-/nfs/chess/id4baux/2026-3}"
NXJOBS_CHESS_LOGO="${NXJOBS_CHESS_LOGO:-/nfs/chess/id4baux/chesslogo.png}"

# --- optional SSH config file (keep all NXJOBS_SSH_* settings in one place) --
# Point NXJOBS_SSH_CONF at EITHER:
#   * a FILE — an env file that sets NXJOBS_SSH_* lines (sourced as-is), OR
#   * a DIR  — a folder of per-user env files; we auto-pick "<youruser>.env"
#              and fall back to "default.env".
# If NXJOBS_SSH_CONF is unset we also look in a few sensible default spots.
# This is how you support different CLASSE users from ONE shared folder:
#   put ss3428.env, jdoe.env, asmith.env, ... in the folder; each person's
#   run auto-loads their own file. No per-user editing of this script.
_me="${USER:-$(id -un)}"
_pick_conf() {
    local c="$1"
    if [[ -d "$c" ]]; then
        [[ -f "$c/${_me}.env" ]]   && { echo "$c/${_me}.env";   return; }
        [[ -f "$c/default.env" ]]  && { echo "$c/default.env";  return; }
        return
    fi
    [[ -f "$c" ]] && echo "$c"
}

NXJOBS_SSH_CONF_FILE=""
if [[ -n "${NXJOBS_SSH_CONF:-}" ]]; then
    NXJOBS_SSH_CONF_FILE="$(_pick_conf "${NXJOBS_SSH_CONF}")"
    if [[ -z "${NXJOBS_SSH_CONF_FILE}" ]]; then
        echo "WARNING: NXJOBS_SSH_CONF='${NXJOBS_SSH_CONF}' had no usable env file" >&2
        echo "         (looked for ${_me}.env then default.env if it's a folder)." >&2
    fi
else
    for _cand in \
        "${SCRIPT_DIR}/nxjobs_ssh.env" \
        "${HOME}/.config/nxjobs/ssh.env" \
        "/nfs/chess/id4baux/nxserver/nxjobs/${_me}.env" \
        "/nfs/chess/id4baux/nxserver/nxjobs/default.env"; do
        [[ -f "${_cand}" ]] && { NXJOBS_SSH_CONF_FILE="${_cand}"; break; }
    done
fi

if [[ -n "${NXJOBS_SSH_CONF_FILE}" ]]; then
    echo "==> Loading SSH config: ${NXJOBS_SSH_CONF_FILE}"
    set -a                         # export everything the file defines
    # shellcheck disable=SC1090
    source "${NXJOBS_SSH_CONF_FILE}"
    set +a
fi

# --- SSH submission config (defaults make it per-user automatically) ---------
# Anything the config file above set is kept; anything it didn't gets a default.
NXJOBS_SSH_ENABLE="${NXJOBS_SSH_ENABLE:-0}"
NXJOBS_SSH_HOST="${NXJOBS_SSH_HOST:-lnx201.classe.cornell.edu}"
NXJOBS_SSH_USER="${NXJOBS_SSH_USER:-${USER:-$(id -un)}}"   # default: whoever runs this
NXJOBS_SSH_PORT="${NXJOBS_SSH_PORT:-22}"
NXJOBS_NXSETUP="${NXJOBS_NXSETUP:-/nfs/chess/id4baux/nxserver/nxsetup.sh}"
# Auto-pick a private key if the user didn't name one.
if [[ -z "${NXJOBS_SSH_KEY:-}" ]]; then
    for _k in "${HOME}/.ssh/id_ed25519" "${HOME}/.ssh/id_rsa"; do
        [[ -f "${_k}" ]] && { NXJOBS_SSH_KEY="${_k}"; break; }
    done
fi
NXJOBS_SSH_KEY="${NXJOBS_SSH_KEY:-}"

# --- sanity checks -----------------------------------------------------------
if ! command -v apptainer >/dev/null 2>&1; then
    echo "ERROR: 'apptainer' not found on PATH." >&2
    echo "       On CLASSE/CHESS try:  module load apptainer" >&2
    exit 1
fi

if [[ ! -f "${SIF}" ]]; then
    echo "ERROR: image not found: ${SIF}" >&2
    echo "       Build it first:  deploy/apptainer/build.sh" >&2
    exit 1
fi

# Warn (don't fail) if the bind source is missing — the app still starts but
# browsing/submitting won't work without the share.
BIND_SRC="${BIND%%:*}"
if [[ ! -d "${BIND_SRC}" ]]; then
    echo "WARNING: bind source '${BIND_SRC}' does not exist on this node." >&2
    echo "         The app will start but cannot read/write the shared data." >&2
fi

# --- assemble the apptainer command -----------------------------------------
ARGS=( run
    --bind "${BIND}"
    --env "NXJOBS_ROOT=${NXJOBS_ROOT}"
    --env "NXJOBS_CHESS_LOGO=${NXJOBS_CHESS_LOGO}"
)

SSH_STATUS="off (submitting locally)"
case "${NXJOBS_SSH_ENABLE}" in
    1|[Tt]rue|[Yy]es|[Oo]n)
        ARGS+=(
            --env "NXJOBS_SSH_ENABLE=1"
            --env "NXJOBS_SSH_HOST=${NXJOBS_SSH_HOST}"
            --env "NXJOBS_SSH_USER=${NXJOBS_SSH_USER}"
            --env "NXJOBS_SSH_PORT=${NXJOBS_SSH_PORT}"
            --env "NXJOBS_NXSETUP=${NXJOBS_NXSETUP}"
        )
        SSH_STATUS="on  → ${NXJOBS_SSH_USER}@${NXJOBS_SSH_HOST}:${NXJOBS_SSH_PORT}"

        # Mount the private key read-only and point the app at it inside the image.
        if [[ -n "${NXJOBS_SSH_KEY}" && -f "${NXJOBS_SSH_KEY}" ]]; then
            ARGS+=(
                --bind "${NXJOBS_SSH_KEY}:/run/nxjobs/id_key:ro"
                --env  "NXJOBS_SSH_KEY=/run/nxjobs/id_key"
            )
            # Bring known_hosts along too, if present, so host-key checks pass.
            if [[ -f "${HOME}/.ssh/known_hosts" ]]; then
                ARGS+=( --bind "${HOME}/.ssh/known_hosts:/run/nxjobs/known_hosts:ro" )
            fi
            SSH_STATUS="${SSH_STATUS}  (key: ${NXJOBS_SSH_KEY})"
        else
            SSH_STATUS="${SSH_STATUS}  (no key found — agent/keys in image will be tried)"
            echo "WARNING: SSH enabled but no private key found at ~/.ssh/id_ed25519 or ~/.ssh/id_rsa." >&2
            echo "         Set NXJOBS_SSH_KEY=/path/to/key, or create one:" >&2
            echo "           ssh-keygen -t ed25519 && ssh-copy-id ${NXJOBS_SSH_USER}@${NXJOBS_SSH_HOST}" >&2
        fi
        ;;
esac

echo "==> Starting NxReduce Job Submission"
echo "    image : ${SIF}"
echo "    bind  : ${BIND}"
echo "    root  : ${NXJOBS_ROOT}"
echo "    ssh   : ${SSH_STATUS}"
echo "    URL   : http://$(hostname):5050   (or http://localhost:5050)"
echo "    (press Ctrl-C to stop)"
echo

exec apptainer "${ARGS[@]}" "${SIF}"
