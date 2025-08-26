#!/usr/bin/env python

import sys
from pathlib import Path

hf = Path(sys.argv[1])

if not hf.is_file():
    print("FAIL")
    sys.exit(1)

hf.unlink()

print("OK")
sys.exit(0)
