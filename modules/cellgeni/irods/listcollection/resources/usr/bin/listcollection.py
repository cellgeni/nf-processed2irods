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


def list_collection(session: iRODSSession, collection_path: str, recursive: bool):
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
        if recursive:
            yield from list_collection(session, subcollection.path, recursive=True)


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
        "-r", "--recursive",
        action="store_true",
        help="Recurse into subcollections",
    )
    args = parser.parse_args()

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
                for item in list_collection(session, args.collection, recursive=args.recursive):
                    writer.writerow(item)

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
