#!/usr/bin/env python3
"""Process a single tile identified by its (x, y) letter coordinates."""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(description="Process one tile of a 2-D grid")
    parser.add_argument("x", help="Tile x-coordinate (uppercase letter, e.g. A)")
    parser.add_argument("y", help="Tile y-coordinate (uppercase letter, e.g. A)")
    args = parser.parse_args()

    tile_id = f"{args.x}{args.y}"
    print(f"Tile ({args.x}, {args.y})")
    print(f"  input  : input_{tile_id}.dat")
    print(f"  output : output_{tile_id}.dat")
    print("Done.")


if __name__ == "__main__":
    sys.exit(main())
