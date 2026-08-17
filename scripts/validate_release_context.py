#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read_workspace_version() -> str:
    cargo = tomllib.loads((ROOT / "rust" / "Cargo.toml").read_text(encoding="utf-8"))
    version = cargo["workspace"]["package"].get("version")
    if not isinstance(version, str) or not version:
        raise SystemExit("[error] rust/Cargo.toml workspace.package.version is missing")
    return version


def current_head_sha() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    return result.stdout.strip()


def run_json(command: list[str]) -> dict[str, object]:
    result = subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )
    return json.loads(result.stdout)


def validate_tag_version() -> str:
    github_ref = os.environ.get("GITHUB_REF", "")
    if not github_ref.startswith("refs/tags/v"):
        raise SystemExit(
            f"[error] Release workflows must run from a v-prefixed tag, got {github_ref!r}"
        )

    tag_version = github_ref.removeprefix("refs/tags/v")
    cargo_version = read_workspace_version()
    if tag_version != cargo_version:
        raise SystemExit(
            f"[error] Tag version v{tag_version} does not match Cargo.toml version {cargo_version}"
        )

    print(f"[ok] Release tag matches Cargo.toml version: v{cargo_version}")
    return cargo_version


def validate_build_run(build_run_id: str, expected_workflow: str) -> None:
    repository = os.environ.get("GITHUB_REPOSITORY")
    if not repository:
        raise SystemExit("[error] GITHUB_REPOSITORY is missing")
    head_sha = current_head_sha()

    data = run_json(
        [
            "gh",
            "run",
            "view",
            build_run_id,
            "--repo",
            repository,
            "--json",
            "conclusion,headSha,status,url,workflowName",
        ]
    )

    if data.get("workflowName") != expected_workflow:
        raise SystemExit(
            f"[error] Build run {build_run_id} used workflow {data.get('workflowName')!r}; "
            f"expected {expected_workflow!r}"
        )
    if data.get("status") != "completed" or data.get("conclusion") != "success":
        raise SystemExit(
            f"[error] Build run {build_run_id} is not a successful completed run: {data.get('url')}"
        )
    if data.get("headSha") != head_sha:
        raise SystemExit(
            f"[error] Build run SHA {data.get('headSha')} does not match publish checkout SHA {head_sha}"
        )

    print(f"[ok] Build run {build_run_id} is successful and matches this release SHA")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate trusted release workflow context")
    parser.add_argument("--build-run-id", help="release-build workflow run ID to validate")
    parser.add_argument("--expected-workflow", default="release-build")
    args = parser.parse_args()

    validate_tag_version()
    if args.build_run_id:
        validate_build_run(args.build_run_id, args.expected_workflow)


if __name__ == "__main__":
    main()
