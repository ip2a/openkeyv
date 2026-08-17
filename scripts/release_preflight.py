#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CRATE_MANIFEST = ROOT / "rust" / "crates" / "openkeyv" / "Cargo.toml"
WORKSPACE_MANIFEST = ROOT / "rust" / "Cargo.toml"
CARGO_LOCK = ROOT / "rust" / "Cargo.lock"


def read_required_string(data: dict[str, object], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise SystemExit(f"[error] missing required string field: {key}")
    return value


def read_workspace_version() -> str:
    workspace = tomllib.loads(WORKSPACE_MANIFEST.read_text(encoding="utf-8"))
    return read_required_string(workspace["workspace"]["package"], "version")


def read_crate_package() -> dict[str, object]:
    return tomllib.loads(CRATE_MANIFEST.read_text(encoding="utf-8"))["package"]


def validate_cargo_metadata(version: str) -> None:
    crate_package = read_crate_package()
    if read_required_string(crate_package, "name") != "openkeyv":
        raise SystemExit("[error] crate package name must be openkeyv")

    version_field = crate_package.get("version")
    if version_field != {"workspace": True}:
        raise SystemExit("[error] crate version must use version.workspace = true")

    readme = read_required_string(crate_package, "readme")
    readme_path = (CRATE_MANIFEST.parent / readme).resolve()
    if not readme_path.exists():
        raise SystemExit(f"[error] crate readme file is missing: {readme_path}")

    repository = read_required_string(crate_package, "repository")
    if "github.com/ip2a/openkeyv" not in repository:
        raise SystemExit(f"[error] crate repository must point to ip2a/openkeyv, got {repository!r}")

    print("[ok] Cargo package metadata is complete for release")


def validate_lockfile_version(version: str) -> None:
    lock_text = CARGO_LOCK.read_text(encoding="utf-8")
    match = re.search(
        r'^\[\[package\]\]\nname = "openkeyv"\nversion = "([^"]+)"',
        lock_text,
        flags=re.MULTILINE,
    )
    if not match:
        raise SystemExit("[error] openkeyv entry not found in rust/Cargo.lock")
    if match.group(1) != version:
        raise SystemExit(
            f"[error] Cargo.lock openkeyv version {match.group(1)!r} "
            f"does not match workspace version {version!r}"
        )
    print("[ok] Cargo.lock matches workspace version")


def validate_release_workflows() -> None:
    expected = {
        "release-build": ROOT / ".github" / "workflows" / "release-build.yml",
        "release-publish-crates": ROOT / ".github" / "workflows" / "release-publish-crates.yml",
        "post-release-verify": ROOT / ".github" / "workflows" / "post-release-verify.yml",
    }
    for name, path in expected.items():
        if not path.exists():
            raise SystemExit(f"[error] missing workflow file for {name}: {path}")
        text = path.read_text(encoding="utf-8")
        if "openkeyv" not in text:
            raise SystemExit(f"[error] workflow {path.name} does not reference openkeyv")
    print("[ok] release workflow files are present")


def validate_crate_package_list(version: str) -> None:
    import subprocess

    result = subprocess.run(
        [
            "cargo",
            "package",
            "--list",
            "--allow-dirty",
            "--manifest-path",
            str(CRATE_MANIFEST),
        ],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    files = set(result.stdout.splitlines())
    required = {"Cargo.toml", "src/lib.rs", "README.md"}
    missing = sorted(required - files)
    if missing:
        raise SystemExit(f"[error] cargo package is missing required files: {missing}")
    print(f"[ok] cargo package manifest includes release assets for v{version}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run trusted release metadata preflight checks")
    parser.add_argument("--skip-package-list", action="store_true")
    args = parser.parse_args()

    version = read_workspace_version()
    validate_cargo_metadata(version)
    validate_lockfile_version(version)
    validate_release_workflows()
    if not args.skip_package_list:
        validate_crate_package_list(version)

    print(f"[ok] Release preflight passed for v{version}")


if __name__ == "__main__":
    main()
