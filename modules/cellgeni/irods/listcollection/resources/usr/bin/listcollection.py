#!/usr/bin/env python3

import argparse
import csv
import os
import sys
from pathlib import Path

from irods.session import iRODSSession
from irods.exception import CollectionDoesNotExist, DataObjectDoesNotExist


def make_session(env_path: Path) -> iRODSSession:
    return iRODSSession(irods_env_file=str(env_path))


def list_collection(session: iRODSSession, collection_path: str, depth: int):
    collection = session.collections.get(collection_path)

    for obj in collection.data_objects:
        yield {
            "type": "data_object",
            "path": f"{collection.path}/{obj.name}",
            "size": obj.size,
            "checksum": obj.checksum or "",
        }

    for subcollection in collection.subcollections:
        yield {
            "type": "collection",
            "path": subcollection.path,
            "size": "",
            "checksum": "",
        }
        # depth counts how many levels to list: depth=1 lists only this level,
        # so we recurse into subcollections only while depth is still > 1.
        if depth > 1:
            yield from list_collection(session, subcollection.path, depth=depth - 1)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="List an iRODS collection and save results to a CSV file."
    )
    parser.add_argument("collection", help="iRODS collection path, e.g. /seq/...")
    parser.add_argument(
        "--env-file",
        help="Path to irods_environment.json. Defaults to ~/.irods/irods_environment.json",
    )
    parser.add_argument(
        "-o", "--output",
        help="Output CSV file path. Defaults to <collection_name>.csv",
    )
    parser.add_argument(
        "-d", "--depth",
        type=int,
        default=1,
        help="Number of collection levels to list. 1 (default) lists only the "
             "given collection; higher values recurse that many levels deep.",
    )
    parser.add_argument(
        "--no-exit",
        action="store_true",
        help="If the collection does not exist, write an empty listing "
             "(header only) and exit 0 instead of failing with exit 2",
    )
    args = parser.parse_args()

    if args.depth < 1:
        parser.error("--depth must be >= 1")

    output_path = Path(args.output) if args.output else Path(args.collection.rstrip("/").split("/")[-1] + ".csv")

    try:
        env_path = Path(
            args.env_file
            or os.environ.get(
                "IRODS_ENVIRONMENT_FILE", Path.home() / ".irods" / "irods_environment.json"
            )
        )
        if not env_path.exists():
            raise FileNotFoundError(f"Cannot find iRODS environment file: {env_path}")

        with make_session(env_path) as session:
            with output_path.open("w", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=["type", "path", "size", "checksum"])
                writer.writeheader()
                try:
                    for item in list_collection(session, args.collection, depth=args.depth):
                        writer.writerow(item)
                except (CollectionDoesNotExist, DataObjectDoesNotExist):
                    # With --no-exit a missing collection is not an error: the
                    # empty listing (header only) has already been written, so
                    # just report it and exit cleanly. Otherwise re-raise so the
                    # caller sees the original exit-2 failure.
                    if not args.no_exit:
                        raise
                    print(f"Collection does not exist yet, empty listing: {args.collection}", file=sys.stderr)

        print(f"Written to {output_path}", file=sys.stderr)

    except CollectionDoesNotExist:
        print(f"Collection does not exist: {args.collection}", file=sys.stderr)
        return 2
    except DataObjectDoesNotExist:
        print(f"Data object does not exist: {args.collection}", file=sys.stderr)
        return 2
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
