# Development Strategy: Arrow Variant Encoding/Decoding

> Issues: #45946 (decoding) → #45947 (encoding)
> Repo: `apache/arrow` (local clone at `C:\...\arrow`)

---

## Build & Test Environment: Docker Containers

No native C++ toolchain on this machine (no compiler, no CMake, no conda). Docker is installed and the Arrow repo ships a full `compose.yaml` with pre-configured build containers.

### Workflow

Edit source files locally in the IDE on Windows. Build and test inside the container — the source tree mounts at `/arrow`:

```bash
# First time: build the container image (pulls deps, takes a while)
docker compose build conda-cpp

# Build libarrow + libparquet and run C++ tests
docker compose run --rm conda-cpp
```

The `conda-cpp` service:
- Uses conda environment with all C++ dependencies pre-installed
- Runs `ci/scripts/cpp_build.sh` then `ci/scripts/cpp_test.sh`
- Mounts `.:/arrow:delegated` so local edits are immediately visible
- Uses a named volume for ccache — subsequent builds are much faster

### Running Specific Tests

To iterate faster than running the full test suite, override the container command:

```bash
# Drop into a shell inside the container
docker compose run --rm conda-cpp bash

# Then inside the container:
cd /build
ninja -j4                           # rebuild
ctest -R "variant" --output-on-failure  # run only variant-related tests
```

### Alternative Containers

| Service | Use Case |
|---------|----------|
| `conda-cpp` | Standard build + full test suite (recommended) |
| `conda-cpp-valgrind` | Memory leak detection |
| `ubuntu-cpp-sanitizer` | ASAN + UBSAN (catches undefined behavior) |
| `debian-cpp` | Tests against system packages instead of conda |

### Environment Variables

Override build options via `-e` flags:

```bash
# Release build (faster runtime, slower compile)
docker compose run --rm -e ARROW_BUILD_TYPE=release conda-cpp

# Build only what's needed for Parquet variant work
docker compose run --rm \
  -e ARROW_FLIGHT=OFF \
  -e ARROW_GANDIVA=OFF \
  -e ARROW_S3=OFF \
  conda-cpp
```

---

## Branching Strategy: Stacked PRs

Issues #45946 and #45947 are tightly coupled (decoder/encoder are inverse operations sharing the same binary format spec) but should be separate PRs per Arrow project convention.

### Git Flow

```bash
# 1. Start decoder work
git checkout main
git pull upstream main
git checkout -b variant-decoding    # PR for #45946

# ... implement decoder, commit, push
git push -u origin variant-decoding
# → Open PR on GitHub targeting main

# 2. Start encoder work, branched off decoder
git checkout -b variant-encoding    # branches from variant-decoding tip

# ... implement encoder, commit, push
git push -u origin variant-encoding
# → Open PR on GitHub with base branch set to "variant-decoding" (NOT main)
```

### Why Branch Off the Decoder

- Encoder tests need the decoder for round-trip verification: `decode(encode(v)) == v`
- Shared internal code (format constants, header parsing, offset size logic) lives in a common header like `variant_encoding_internal.h` — authored in PR #1, used by both
- PR #2's diff stays clean — only shows encoder-specific changes

### Handling Review Feedback

```bash
# If decoder PR gets review changes that affect shared code:
git checkout variant-decoding
# ... address comments, commit, push

# Rebase encoder on top of updated decoder
git checkout variant-encoding
git rebase variant-decoding
git push --force-with-lease
```

### After PR #1 Merges

Once the decoder PR merges into `main`, retarget PR #2:

```bash
git checkout variant-encoding
git rebase main
git push --force-with-lease
# → Update PR #2 base branch to "main" on GitHub
```

GitHub may do this automatically when the base branch merges, but manual rebase keeps things clean.

### PR Conventions for Arrow

- Link both PRs to umbrella issue #45937
- Title format: `GH-45946: [C++][Parquet] Variant decoding`
- In PR #2 description, note dependency: "Depends on #<PR1_number>"
- Reviewers expect stacked PRs — they'll review in order
- Keep PRs focused: decoder is ~800-1200 lines, encoder ~600-900 lines (per analysis doc estimates)

---

## Summary: Day-to-Day Loop

1. Edit C++ source files in IDE (Windows)
2. `docker compose run --rm conda-cpp bash` → interactive shell
3. `cd /build && ninja -j4` → incremental rebuild
4. `ctest -R "variant" --output-on-failure` → run targeted tests
5. Iterate until green
6. Commit, push, get review feedback
7. Repeat
