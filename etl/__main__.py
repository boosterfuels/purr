#!/usr/bin/env python3

import sys


def main():
    try:
        from etl.cli import main
        sys.exit(main())
    except KeyboardInterrupt:
        print('\nInterrupted by user.')


if __name__ == '__main__':
    main()
