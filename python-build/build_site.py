import contextlib
import json
import os
import pathlib
import shutil
import subprocess
from datetime import datetime, timezone

PYTHON_BUILD_DIR = pathlib.Path(__file__).parent
WHEEL_DIR = PYTHON_BUILD_DIR / "wheels"
SITE_OUTPUT_DIR = PYTHON_BUILD_DIR / "site"

shutil.rmtree(SITE_OUTPUT_DIR, ignore_errors=True)
SITE_OUTPUT_DIR.mkdir(parents=True)

SITE_ARTIFACT_WHEEL_DIR = SITE_OUTPUT_DIR / "artifacts" / "wheels"
SITE_ARTIFACT_WHEEL_DIR.mkdir(parents=True)
for wheel_file in WHEEL_DIR.glob("*"):
    shutil.copy(wheel_file, SITE_ARTIFACT_WHEEL_DIR)


def package_name(wheel_file: pathlib.Path) -> str:
    return wheel_file.name.split("-")[0].replace("_", "-")


# Get some extra context about the build
ts = datetime.now(timezone.utc).isoformat()
context_info: dict = {
    "timestamp": ts,
}

# Get branch info.
with contextlib.suppress(Exception):
    if branch_info := os.getenv("GITHUB_HEAD_REF"):
        pass
    else:
        branch_info = subprocess.check_output(
            ["git", "branch", "--show-current"], text=True
        )
    context_info["branch"] = branch_info.strip()

# Get commit info.
with contextlib.suppress(Exception):
    commit_info = subprocess.check_output(
        ["git", "log", "-1", "--pretty=%H%n%B"], text=True
    )
    commit_hash, commit_msg = commit_info.strip().split("\n", 1)
    context_info["commit"] = {
        "hash": commit_hash,
        "message": commit_msg.strip(),
    }

# Get PR info.
with contextlib.suppress(Exception):
    pr_info = "unknown"
    if github_ref := os.getenv("GITHUB_REF"):
        # e.g. GITHUB_REF=refs/pull/12157/merge
        parts = github_ref.split("/")
        if parts[1] == "pull":
            pull_number = parts[2]
            pr_info = json.loads(
                subprocess.check_output(
                    ["gh", "pr", "view", pull_number, "--json", "title,number,url"],
                    text=True,
                )
            )
    else:
        # The `gh` CLI might be able to figure it out.
        pr_info = json.loads(
            subprocess.check_output(
                ["gh", "pr", "view", "--json", "title,number,url"], text=True
            )
        )
    context_info["pr"] = pr_info


newline = "\n"
(SITE_OUTPUT_DIR / "index.html").write_text(
    f"""
<html>
  <head>
    <title>DataHub Python Builds</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">

    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/semantic-ui@2.5.0/dist/semantic.min.css" integrity="sha256-cDGQ39yChhpN5vzgHbjIdGEtQ5kXE9tttCsI7VR9TuY=" crossorigin="anonymous">
    <script src="https://cdn.jsdelivr.net/npm/semantic-ui@2.5.0/dist/semantic.min.js" integrity="sha256-fN8vcX2ULyTDspVTHEteK8hd3rQAb5thNiwakjAW75Q=" crossorigin="anonymous"></script>

    <!-- CDN example (jsDelivr) -->
    <script src="https://cdn.jsdelivr.net/npm/dayjs@1.11.13/dayjs.min.js" integrity="sha256-nP25Pzivzy0Har7NZtMr/TODzfGWdlTrwmomYF2vQXM=" crossorigin="anonymous"></script>
    <script src="https://cdn.jsdelivr.net/npm/dayjs@1.11.13/plugin/relativeTime.js" integrity="sha256-muryXOPFkVJcJO1YFmhuKyXYmGDT2TYVxivG0MCgRzg=" crossorigin="anonymous"></script>
    <script>dayjs.extend(window.dayjs_plugin_relativeTime)</script>
  </head>
  <body>
    <div class="ui container">
      <h1 class="ui header" style="padding-top: 1.5em;">DataHub Python Builds</h1>
      <p>
        These prebuilt wheel files can be used to install our Python packages as of a specific commit.
      </p>

      <h2>Build context</h2>
      <p>
        Built <span id="build-timestamp">at {ts}</span>.
      </p>
      <pre id="context-info">{json.dumps(context_info, indent=2)}</pre>

      <h2>Usage</h2>
      <p>
      Current base URL: <span class="base-url">unknown</span>
      </p>

      <table class="ui celled table">
      <thead>
        <tr>
          <th>Package</th>
          <th>Size</th>
          <th>Install command</th>
        </tr>
      </thead>
      <tbody>
        {
        newline.join(
            f'''
            <tr>
              <td><code>{package_name(wheel_file)}</code></td>
              <td>{wheel_file.stat().st_size / 1024 / 1024:.3f} MB</td>
              <td><code>uv pip install '{package_name(wheel_file)} @ <span class="base-url">&lt;base-url&gt;</span>/artifacts/wheels/{wheel_file.name}'</code></td>
            </tr>
            '''
            for wheel_file in sorted(WHEEL_DIR.glob("*.whl"))
        )
        }
      </tbody>
      </table>
    </div>
  </body>
  <script>
    const baseUrl = window.location.href.split('/').slice(0, -1).join('/');
    document.querySelectorAll(".base-url").forEach(el => {{
      el.textContent = baseUrl;
    }});

    const buildTimestamp = document.getElementById("build-timestamp");
    const buildTimestampDate = dayjs('{ts}');
    buildTimestamp.textContent = buildTimestampDate.fromNow();
  </script>
</html>
"""
)

print("DataHub Python wheel site built in", SITE_OUTPUT_DIR)
