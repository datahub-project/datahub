#!/bin/bash
# Fix broken compiled extension modules in datahub-executor .venv
# This script reinstalls all packages with compiled C/C++ extensions that commonly break

set -e

echo "🔧 Fixing compiled extension modules in datahub-executor/.venv"
echo "=================================================="

# Ensure we're in the datahub-executor directory
cd "$(dirname "$0")/.."

# Activate virtual environment
source .venv/bin/activate

echo "📦 Step 1: Reinstalling core numerical packages..."
pip install --force-reinstall --no-cache-dir \
    cffi==1.17.1 \
    cryptography==44.0.0 \
    numpy==1.26.4 \
    pandas==2.1.4

echo "📦 Step 2: Reinstalling scientific computing packages..."
pip install --force-reinstall --no-cache-dir --no-deps \
    scipy==1.14.1 \
    scikit-learn==1.6.1

echo "📦 Step 3: Reinstalling data processing packages..."
pip install --force-reinstall --no-cache-dir \
    pyarrow==18.1.0 \
    lz4==4.3.3 \
    grpcio==1.68.1

echo "📦 Step 4: Reinstalling NLP packages (spacy)..."
pip install --force-reinstall --no-cache-dir --no-deps \
    blis==0.7.11 \
    cymem==2.0.10 \
    murmurhash==1.0.11 \
    preshed==3.0.9 \
    srsly==2.4.8 \
    thinc==8.2.5 \
    spacy==3.7.5

echo "📦 Step 5: Reinstalling system packages..."
pip install --force-reinstall --no-cache-dir \
    greenlet==3.1.1 \
    psutil==7.0.0 \
    pyzmq==26.2.0

echo "📦 Step 6: Reinstalling pydantic..."
pip install --force-reinstall --no-cache-dir \
    pydantic-core==2.33.2 \
    pydantic==2.11.9

echo "📦 Step 7: Fixing version conflicts..."
pip install \
    pytz==2024.1 \
    typing-extensions==4.12.2

echo ""
echo "✅ All compiled extension modules have been reinstalled!"
echo "=================================================="
echo ""
echo "You can now run: streamlit run scripts/assertions_ui.py"

