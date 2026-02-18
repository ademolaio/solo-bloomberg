from __future__ import annotations

from .macro_registry_schema import main as build_macro_registry
from .fred_schema import main as build_fred_schema


def main():
    build_macro_registry()
    build_fred_schema()
    print("[ok] build_all: macro registry + fred provider schema built")


if __name__ == "__main__":
    main()