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

## Environment Variables

Some tests require environment variables to be set. For local development, you have two options:

### Option 1: Using 1Password (Recommended for Acryl Engineers)

If you have access to the Acryl 1Password vault, you can use the provided scripts to automatically generate a `.env` file with the necessary secrets:

1. Make sure you have 1Password CLI installed and configured:

   ```bash
   brew install 1password-cli
   ```

2. Run the environment generation script:

   ```bash
   ./env_gen.sh
   ```

   This will create a `.env` file with all the necessary secrets from 1Password.

3. Run the tests:
   ```bash
   pytest your_test_file.py
   ```

### Option 2: Manual .env File

If you don't have access to 1Password or prefer to set up manually:

1. Copy the `.env.example` file to `.env`:

   ```
   cp .env.example .env
   ```

2. Edit the `.env` file and fill in your values:

   ```
   MIXPANEL_API_SECRET=your_mixpanel_api_secret_here
   MIXPANEL_PROJECT_ID=3653440
   ```

3. The tests will automatically load these environment variables.

Note: The `.env` file is ignored by git, so your secrets won't be committed to the repository.
