import pathlib
import shutil
from datetime import datetime

PYTHON_BUILD_DIR = pathlib.Path(__file__).parent
WHEEL_DIR = PYTHON_BUILD_DIR / "wheels"
SITE_OUTPUT_DIR = PYTHON_BUILD_DIR / "site"

shutil.rmtree(SITE_OUTPUT_DIR, ignore_errors=True)
SITE_OUTPUT_DIR.mkdir(parents=True)

SITE_ARTIFACT_WHEEL_DIR = SITE_OUTPUT_DIR / "artifacts" / "wheels"
SITE_ARTIFACT_WHEEL_DIR.mkdir(parents=True)
for wheel_file in WHEEL_DIR.glob("*"):
    shutil.copy(wheel_file, SITE_ARTIFACT_WHEEL_DIR)

newline = "\n"
(SITE_OUTPUT_DIR / "index.html").write_text(
    f"""
<html>
  <head>
    <title>DataHub Python Builds</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0">

    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/semantic-ui@2.5.0/dist/semantic.min.css" integrity="sha256-cDGQ39yChhpN5vzgHbjIdGEtQ5kXE9tttCsI7VR9TuY=" crossorigin="anonymous">
    <script src="https://cdn.jsdelivr.net/npm/semantic-ui@2.5.0/dist/semantic.min.js" integrity="sha256-fN8vcX2ULyTDspVTHEteK8hd3rQAb5thNiwakjAW75Q=" crossorigin="anonymous"></script>
  </head>
  <body>
    <div class="ui container">
      <h1 class="ui header" style="padding-top: 1.5em;">DataHub Python Builds</h1>
      <p>Built at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>

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
              <td><code>{wheel_file.name.split('-')[0].replace('_', '-')}</code></td>
              <td>{wheel_file.stat().st_size / 1024 / 1024:.3f} MB</td>
              <td><code>pip install '<span class="base-url">&lt;base-url&gt;</span>/artifacts/wheels/{wheel_file.name}'</code></td>
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
    document.querySelectorAll(".base-url").forEach(el => {{
      el.textContent = window.location.href.split('/').slice(0, -1).join('/');
    }});
  </script>
</html>
"""
)

print("Built site in", SITE_OUTPUT_DIR)
