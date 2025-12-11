#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

"""Validate that gradle.lockfile files are up-to-date with current dependencies.

This script uses Gradle's built-in dependency resolution to regenerate lockfiles
and checks if they differ from the committed versions.
"""

import subprocess
import sys
import os


def run_command(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=check,
    )


def check_for_changes_in_gradle_files() -> bool:
    """Check if any build.gradle or gradle.lockfile files were changed."""
    try:
        # Get all changed files (staged + unstaged)
        result = run_command(["git", "diff", "--name-only", "HEAD"])
        unstaged = set(result.stdout.strip().split("\n")) if result.stdout.strip() else set()

        result = run_command(["git", "diff", "--name-only", "--cached"])
        staged = set(result.stdout.strip().split("\n")) if result.stdout.strip() else set()

        changed_files = unstaged.union(staged)

        # Check if any gradle files were modified
        gradle_files = [
            f for f in changed_files
            if f.endswith("build.gradle")
            or f.endswith("build.gradle.kts")
            or f.endswith("gradle.lockfile")
            or "gradle.properties" in f
            or "gradle/wrapper" in f
        ]

        return len(gradle_files) > 0
    except subprocess.CalledProcessError:
        # If we can't determine, assume we should check
        return True


def regenerate_lockfiles() -> tuple[bool, str]:
    """Regenerate all lockfiles using Gradle.

    Returns:
        Tuple of (success, error_message)
    """
    print("Regenerating lockfiles to verify they are up-to-date...")
    print("Running: ./gradlew resolveAndLockAll --write-locks")

    try:
        result = run_command(
            ["./gradlew", "resolveAndLockAll", "--write-locks", "-x", "generateGitPropertiesGlobal"],
            check=False
        )

        if result.returncode != 0:
            return False, f"Failed to regenerate lockfiles:\n{result.stderr}"

        return True, ""
    except Exception as e:
        return False, f"Error running Gradle: {str(e)}"


def check_for_lockfile_diffs() -> tuple[bool, list[str]]:
    """Check if any lockfiles have differences after regeneration.

    Returns:
        Tuple of (has_diffs, list_of_changed_files)
    """
    try:
        result = run_command(
            ["git", "diff", "--name-only", "**gradle.lockfile"],
            check=False
        )

        if result.returncode == 0 and result.stdout.strip():
            changed_lockfiles = [
                f for f in result.stdout.strip().split("\n")
                if f.endswith("gradle.lockfile")
            ]
            return True, changed_lockfiles

        return False, []
    except subprocess.CalledProcessError:
        return False, []


def restore_lockfiles():
    """Restore lockfiles to their original state."""
    print("Restoring lockfiles to original state...")
    try:
        run_command(["git", "checkout", "**gradle.lockfile"], check=False)
    except:
        pass


def main():
    """Main validation function."""
    print("Checking gradle lockfile updates...")

    # Check if we're in a git repository
    result = run_command(["git", "rev-parse", "--git-dir"], check=False)
    if result.returncode != 0:
        print("Not in a git repository. Skipping lockfile check.")
        return 0

    # Check if any gradle-related files changed
    if not check_for_changes_in_gradle_files():
        print("✓ No gradle files changed. Skipping lockfile verification.")
        return 0

    # Regenerate lockfiles
    success, error = regenerate_lockfiles()
    if not success:
        print(f"\n❌ ERROR: {error}")
        return 1

    # Check for differences
    has_diffs, changed_files = check_for_lockfile_diffs()

    # Always restore lockfiles to original state
    restore_lockfiles()

    if has_diffs:
        print("\n❌ ERROR: Dependency lockfiles are out of date!\n")
        print("The following lockfiles need to be updated:\n")
        for file in changed_files:
            print(f"  • {file}")

        print("\nYour build.gradle changes affect dependency resolution, but the")
        print("corresponding lockfiles were not updated.\n")
        print("To fix this, run:")
        print("  ./gradlew resolveAndLockAll --write-locks\n")
        print("Then commit the updated gradle.lockfile files along with your changes.")

        return 1

    print("✓ All gradle lockfiles are up-to-date.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
