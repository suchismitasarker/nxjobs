#!/usr/bin/env bash
#
# Build the Apptainer image (.sif) for the NxReduce Job Submission app.
#
# Apptainer (formerly Singularity) is the container runtime that beamline /
# HPC login + compute nodes typically allow (rootless, no Docker daemon). This
# script wraps the multi-stage `apptainer build` so you don't have to remember
# the context rules.
#
# USAGE
#   cd script-nxsjobs/final
#   deploy/apptainer/build.sh                 # -> deploy/apptainer/nxjobs.sif
#   deploy/apptainer/build.sh /tmp/out.sif    # custom output path
#
# REQUIREMENTS
#   * apptainer >= 1.1   (needed for the multi-stage .def; `apptainer version`)
#   * The build context is script-nxsjobs/final/ (the folder that directly holds
#     nxjobs-app.py and this deploy/ folder). This script computes that for you,
#     so you can run it from anywhere.
#
# NOTES
#   * The build compiles wheels (h5py, cryptography) in a throwaway "build"
#     stage and copies only the finished /opt/venv into the final image, so the
#     .sif stays small.
#   * No root is required for `apptainer build` on most CLASSE/CHESS nodes; if
#     your site needs it, use `--fakeroot` (added automatically below when the
#     user is not root and fakeroot is configured).

set -euo pipefail

# --- locate things -----------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"      # .../deploy/apptainer
DEF_FILE="${SCRIPT_DIR}/nxjobs.def"
OUT_SIF="${1:-${SCRIPT_DIR}/nxjobs.sif}"

# The build context is the folder that holds nxjobs-app.py + deploy/, i.e. two
# levels up from this script (script-nxsjobs/final/).
CONTEXT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# --- sanity checks -----------------------------------------------------------
if ! command -v apptainer >/dev/null 2>&1; then
    echo "ERROR: 'apptainer' not found on PATH." >&2
    echo "       On CLASSE/CHESS try:  module load apptainer   (or singularity)" >&2
    exit 1
fi

if [[ ! -f "${CONTEXT_DIR}/nxjobs-app.py" ]]; then
    echo "ERROR: ${CONTEXT_DIR}/nxjobs-app.py not found." >&2
    echo "       This deploy/ folder must sit next to nxjobs-app.py." >&2
    exit 1
fi

if [[ ! -f "${CONTEXT_DIR}/deploy/wsgi.py" ]]; then
    echo "ERROR: ${CONTEXT_DIR}/deploy/wsgi.py not found." >&2
    exit 1
fi

# --- decide whether we need --fakeroot ---------------------------------------
BUILD_FLAGS=()
if [[ "$(id -u)" -ne 0 ]]; then
    # Rootless build: most modern Apptainer installs handle the def's apt-get
    # steps via user namespaces. If your site requires it, --fakeroot is safe.
    if apptainer build --help 2>/dev/null | grep -q -- "--fakeroot"; then
        BUILD_FLAGS+=(--fakeroot)
    fi
fi

echo "==> Building Apptainer image"
echo "    def file : ${DEF_FILE}"
echo "    context  : ${CONTEXT_DIR}"
echo "    output   : ${OUT_SIF}"
echo "    flags    : ${BUILD_FLAGS[*]:-(none)}"
echo

# IMPORTANT: run from the context dir so %files paths resolve correctly.
cd "${CONTEXT_DIR}"
apptainer build "${BUILD_FLAGS[@]}" "${OUT_SIF}" "${DEF_FILE}"

echo
echo "==> Done.  Built: ${OUT_SIF}"
echo
echo "Run it (bind-mount the shared filesystem):"
echo "    apptainer run --bind /nfs/chess:/nfs/chess ${OUT_SIF}"
echo
echo "Then open http://localhost:5050  (or the node's hostname:5050)."
