#!/usr/bin/env bash
# Build Mooncake data_manager_v2 and produce a CUDA-13 wheel.
# Every command failure aborts immediately through `set -Eeuo pipefail`.

set -Eeuo pipefail

export GOPROXY="${GOPROXY:-https://mirrors.aliyun.com/goproxy/,direct}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly REPO_ROOT
# scripts/build_wheel.sh hardcodes `build/...` paths, so keep this directory fixed.
readonly BUILD_DIR="${REPO_ROOT}/build"
readonly NVLINK_ALLOCATOR_DIR="${REPO_ROOT}/mooncake-transfer-engine/nvlink-allocator"
readonly NVLINK_OUTPUT_DIR="${BUILD_DIR}/mooncake-transfer-engine/nvlink-allocator"
readonly WHEEL_OUTPUT_SUBDIR="${WHEEL_OUTPUT_SUBDIR:-dist}"
readonly WHEEL_OUTPUT_DIR="${REPO_ROOT}/mooncake-wheel/${WHEEL_OUTPUT_SUBDIR}"

PYTHON_VERSION="${PYTHON_VERSION:-3.12}"
EP_TORCH_VERSIONS="${EP_TORCH_VERSIONS:-2.13.0}"
TORCH_CUDA_ARCH_LIST="${TORCH_CUDA_ARCH_LIST:-9.0}"
BUILD_JOBS="${BUILD_JOBS:-$(nproc)}"
CUDA_HOME="${CUDA_HOME:-/usr/local/cuda}"
CUDA_STUB_DIR="${CUDA_STUB_DIR:-${CUDA_HOME}/lib64/stubs}"

log() {
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

on_error() {
    local status="$1"
    local line="$2"
    local command="$3"
    printf '\nERROR: command failed with exit code %s at line %s:\n  %s\n' \
        "$status" "$line" "$command" >&2
    exit "$status"
}

trap 'on_error "$?" "$LINENO" "$BASH_COMMAND"' ERR

log "Repository: ${REPO_ROOT}"
log "Build directory: ${BUILD_DIR}"
log "Build jobs: ${BUILD_JOBS}"
log "Python wheel version: ${PYTHON_VERSION}"
log "EP torch versions: ${EP_TORCH_VERSIONS}"
log "CUDA architecture list: ${TORCH_CUDA_ARCH_LIST}"

for command_name in cmake make python pip nvcc g++; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
        printf 'ERROR: required command not found: %s\n' "$command_name" >&2
        exit 1
    fi
done

ACTUAL_PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
if [[ "$ACTUAL_PYTHON_VERSION" != "$PYTHON_VERSION" ]]; then
    printf 'ERROR: requested Python %s, but `python` resolved to %s\n' \
        "$PYTHON_VERSION" "$ACTUAL_PYTHON_VERSION" >&2
    exit 1
fi
log "Active Python version: ${ACTUAL_PYTHON_VERSION}"

if ! pip --version | grep -q "(python ${ACTUAL_PYTHON_VERSION})"; then
    printf 'ERROR: `pip` does not target Python %s; check PATH or activate the build environment.\n' \
        "$ACTUAL_PYTHON_VERSION" >&2
    printf 'Current pip: ' >&2
    pip --version >&2
    exit 1
fi

if [[ ! -d "$CUDA_STUB_DIR" ]]; then
    printf 'ERROR: CUDA stub directory not found: %s\n' "$CUDA_STUB_DIR" >&2
    printf '       Set CUDA_HOME or CUDA_STUB_DIR if CUDA is installed elsewhere.\n' >&2
    exit 1
fi

if [[ ! -x "${REPO_ROOT}/scripts/build_wheel.sh" ]]; then
    printf 'ERROR: wheel build script not found or not executable: %s/scripts/build_wheel.sh\n' "$REPO_ROOT" >&2
    exit 1
fi
if [[ ! -f "${NVLINK_ALLOCATOR_DIR}/build.sh" ]]; then
    printf 'ERROR: nvlink allocator build script not found: %s\n' "${NVLINK_ALLOCATOR_DIR}/build.sh" >&2
    exit 1
fi

# `nvlink_allocator.so` links against libcuda.so from the CUDA stubs directory.
export LIBRARY_PATH="${CUDA_STUB_DIR}${LIBRARY_PATH:+:${LIBRARY_PATH}}"
export LD_LIBRARY_PATH="${CUDA_STUB_DIR}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
export LD_LIBRARY_PATH="/usr/local/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"

log "Step 1/4: configuring CMake (Release, CUDA, EP, HTTP, CXL, ETCD)"
mkdir -p "$BUILD_DIR"
cmake \
    -S "$REPO_ROOT" \
    -B "$BUILD_DIR" \
    -DBUILD_UNIT_TESTS=ON \
    -DUSE_HTTP=ON \
    -DUSE_CXL=ON \
    -DUSE_CUDA=ON \
    -DWITH_EP=ON \
    -DUSE_ETCD=ON \
    -DSTORE_USE_ETCD=ON \
    -DCMAKE_BUILD_TYPE=Release \
    --fresh

log "Step 2/4: building all CMake targets"
cmake --build "$BUILD_DIR" --parallel "$BUILD_JOBS"

log "Step 3/4: building nvlink_allocator.so"
mkdir -p "$NVLINK_OUTPUT_DIR"
bash "${NVLINK_ALLOCATOR_DIR}/build.sh" "$NVLINK_OUTPUT_DIR"
if [[ ! -f "${NVLINK_OUTPUT_DIR}/nvlink_allocator.so" ]]; then
    printf 'ERROR: nvlink_allocator.so was not produced at %s\n' "${NVLINK_OUTPUT_DIR}/nvlink_allocator.so" >&2
    exit 1
fi

log "Step 4/4: building mooncake wheel"
# scripts/build_wheel.sh uses paths relative to the Mooncake repository root.
# Enter that directory so this launcher works from any current directory.
(
    cd "$REPO_ROOT"
    CU13_BUILD=1 \
    BUILD_WITH_EP=1 \
    EP_TORCH_VERSIONS="$EP_TORCH_VERSIONS" \
    TORCH_CUDA_ARCH_LIST="$TORCH_CUDA_ARCH_LIST" \
    bash "$REPO_ROOT/scripts/build_wheel.sh" "$PYTHON_VERSION" "$WHEEL_OUTPUT_SUBDIR"
)

shopt -s nullglob
WHEELS=("${WHEEL_OUTPUT_DIR}"/*.whl)
shopt -u nullglob
if ((${#WHEELS[@]} == 0)); then
    printf 'ERROR: no wheel produced in %s\n' "$WHEEL_OUTPUT_DIR" >&2
    exit 1
fi

log "Build completed successfully"
printf 'Wheels:\n'
printf '  %s\n' "${WHEELS[@]}"
