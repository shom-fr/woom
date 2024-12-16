#!/usr/bin/env python3
import sys
from woom.cli import main

if __name__ == "__main__":
    sys.argv = ["./woom", "run", "--clean", "--log-level", "debug", "--session", "all_stages"]
    print(" ".join(sys.argv))
    main()
