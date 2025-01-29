#!/usr/bin/env python3
import sys
from woom.cli import main

if __name__ == "__main__":
    sys.argv = ["./woom", "run", "--log-level", "debug"]
    print(" ".join(sys.argv))
    main()
