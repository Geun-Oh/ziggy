# Releasing ziggy

This document describes the release procedure for ziggy, including validation, tagging, artifact publication, and post-release checks.

## Release goals

A release must:

- pass all required CI checks
- produce Tier 1 release artifacts for all supported OS/architecture targets
- publish checksummed archives to GitHub Releases
- leave a clear changelog and reproducible tag history

## Tier 1 release targets

The current Tier 1 release matrix is:

- `x86_64-linux`
- `aarch64-linux`
- `x86_64-macos`
- `aarch64-macos`

## Pre-release checklist

Before creating a tag, verify all of the following:

- [ ] `main` is green in GitHub Actions
- [ ] `zig test src/ziggy/engine.zig` passes locally
- [ ] `zig build test` passes locally
- [ ] `zig build platform-matrix` passes locally
- [ ] `CHANGELOG.md` is updated
- [ ] `README.md` reflects any platform, CLI, or release changes
- [ ] `SPEC.md`, `TASK.md`, and `TRACEABILITY.md` are updated if release scope changed documented behavior
- [ ] no unintended local debug artifacts are included

## Recommended local verification

```bash
zig test src/ziggy/engine.zig
zig build test
zig build platform-matrix
```

If the release contains storage-format, recovery, or platform changes, also consider:

```bash
zig build
bash scripts/fault_matrix.sh ./zig-out/bin/ziggy ./artifacts/fault-matrix-local
bash scripts/soak_compat.sh ./zig-out/bin/ziggy ./artifacts/soak-local 120
```

## Versioning policy

ziggy aims to follow Semantic Versioning:

- **Patch**: bug fixes, CI fixes, internal hardening without external contract changes
- **Minor**: backward-compatible API or feature additions
- **Major**: breaking API or on-disk format changes

If a release changes on-disk compatibility, call it out explicitly in:

- `CHANGELOG.md`
- GitHub Release notes
- `SPEC.md` / `TASK.md` where relevant

## Release procedure

### 1. Sync main

```bash
git checkout main
git pull --ff-only origin main
```

### 2. Update changelog

Edit `CHANGELOG.md` and move the intended items from `Unreleased` into a versioned section if desired.

### 3. Create the tag

Example:

```bash
git tag v0.1.0
git push origin v0.1.0
```

### 4. Monitor GitHub Actions

The tag push triggers `.github/workflows/release.yml`.

Expected behavior:

- matrix build runs for each Tier 1 target
- release archives are packaged as `.tar.gz`
- SHA-256 checksum files are generated
- GitHub Release is created/updated automatically

## Expected release artifacts

For each target, the workflow publishes:

- `ziggy-<target>.tar.gz`
- `ziggy-<target>.tar.gz.sha256`

Example:

- `ziggy-x86_64-linux.tar.gz`
- `ziggy-x86_64-linux.tar.gz.sha256`

Each archive includes:

- `ziggy`
- `README.md`
- `LICENSE`

## Post-release verification

After the workflow completes:

- [ ] GitHub Release exists for the tag
- [ ] all Tier 1 artifacts are attached
- [ ] all `.sha256` files are attached
- [ ] release notes are populated correctly
- [ ] changelog entry matches the published release scope

Recommended manual smoke check after download:

```bash
tar -xzf ziggy-x86_64-linux.tar.gz
./ziggy-x86_64-linux/ziggy open --path ./release-smoke-db
./ziggy-x86_64-linux/ziggy put --path ./release-smoke-db --key smoke --value ok
./ziggy-x86_64-linux/ziggy get --path ./release-smoke-db --key smoke
```

## Failure handling

### If one target fails in the release matrix

- do not create a replacement tag immediately
- inspect the failed matrix job
- fix the issue on `main`
- create a new tag for the corrected release (preferred), or delete/recreate the tag only if your repository policy permits it

### If release notes are wrong

- edit the GitHub Release body manually
- update `CHANGELOG.md` in `main` if the issue reflects missing project history

### If artifacts are incomplete

- verify matrix completion in Actions
- confirm `softprops/action-gh-release` published all downloaded artifacts
- re-run the workflow from the GitHub UI only if the source commit/tag is still correct

## Notes on platform policy

- Linux uses atomic no-replace checkpoint rename when available
- non-Linux supported targets use the portable checkpoint reservation protocol with fail-closed incomplete checkpoint markers
- unsupported targets must fail fast rather than silently degrade durability semantics
