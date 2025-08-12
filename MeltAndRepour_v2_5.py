#!/usr/bin/env python3
"""
Melt & Repour Global Racing Scanner V2.5 (wrapper)

This is a thin wrapper around `MeltAndRepour_SINGLE.py` so you can run
using a 2.5-style filename without duplicating code.
"""
import asyncio
from MeltAndRepour_SINGLE import parse_args, _amain

def main():
    args = parse_args()
    asyncio.run(_amain(args))

if __name__ == "__main__":
    main()