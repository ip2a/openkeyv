#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run_command(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        **kwargs,
    )


class ReleaseScriptsTest(unittest.TestCase):
    def test_release_metadata_preflight_passes(self) -> None:
        result = run_command(["python3", "scripts/release_preflight.py"])
        self.assertIn("[ok] Release preflight passed", result.stdout)

    def test_validate_release_context_requires_tag(self) -> None:
        env = os.environ.copy()
        env["GITHUB_REF"] = "refs/heads/master"
        with self.assertRaises(subprocess.CalledProcessError):
            run_command(["python3", "scripts/validate_release_context.py"], env=env)


if __name__ == "__main__":
    unittest.main(verbosity=2)
