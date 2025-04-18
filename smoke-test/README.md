# Smoke Test

## Local Development

### Prerequisites

Some tests needs to resolve credentials to work properly.
For that you will need 1password cli installed and configured.

```bash
brew install 1password-cli
```

### How to Run

1. Install the dependencies

```bash
python -m venv venv
python -m pip install -r requirements.txt
```

2. Run the tests

```bash
op run -- pytest your_test_file.py
```
