#!/usr/bin/env python3
"""Aggregate per-sample metadata for a dataset.

Reads all metadata files passed on the command line (STARsolo QC ``*.solo_qc.tsv``
and public accession tables ``*.accessions.tsv`` fetched from GEO/ENA) and produces
a single CSV and JSON file with one record per sample, keyed by the STARsolo sample
name (the ``Sample`` column of the QC table).

The script is deliberately tolerant: any input file may be absent. Only the columns
that are actually present are emitted, so it works for both public and private datasets.
"""

import argparse
import csv
import glob
import json
import sys
from collections import OrderedDict


# Columns lifted verbatim from the STARsolo QC table (solo_qc.tsv), keyed by the
# 'Sample' column. Maps source column -> output field name.
QC_FIELDS = OrderedDict(
    [
        ("Species", "species"),
        ("Paired", "paired"),
        ("Strand", "strand"),
        ("Rd_all", "total_reads"),
        ("WL", "whitelist"),
        ("Cells", "cells"),
    ]
)

# Accession table columns (accessions.tsv): geo_sample, sample, experiment, run.
# These enrich a QC row when the accession 'sample' matches the QC 'Sample' name.
ACCESSION_HEADER = ["geo_sample", "sample", "experiment", "run"]


def read_tsv(path, header=None):
    """Read a TSV file into a list of dict rows.

    If ``header`` is given the file is assumed headerless and those names are used;
    otherwise the first line is treated as the header.
    """
    with open(path, newline="") as fh:
        reader = csv.reader(fh, delimiter="\t")
        rows = [r for r in reader if r and any(cell.strip() for cell in r)]
    if not rows:
        return []
    if header is None:
        header = [h.strip() for h in rows[0]]
        data = rows[1:]
    else:
        data = rows
    return [dict(zip(header, [c.strip() for c in r])) for r in data]


def collect_qc(qc_paths):
    """Build ``{sample_name: {field: value}}`` from all QC tables."""
    samples = OrderedDict()
    for path in qc_paths:
        for row in read_tsv(path):
            name = row.get("Sample")
            if not name:
                continue
            record = samples.setdefault(name, OrderedDict([("sample_id", name)]))
            for src, dst in QC_FIELDS.items():
                if row.get(src):
                    record[dst] = row[src]
    return samples


def collect_accessions(acc_paths):
    """Index accession rows by every identifier that can match a QC sample name.

    Public samples may be referenced in the QC table by either their GEO id
    (``geo_sample``) or their SRA/ENA sample id (``sample``), so both are used as keys.
    """
    index = {}
    for path in acc_paths:
        for row in read_tsv(path, header=ACCESSION_HEADER):
            enrich = OrderedDict()
            for field in ACCESSION_HEADER:
                value = row.get(field, "").strip()
                if value and value != "-":
                    enrich[field] = value
            for key_field in ("sample", "geo_sample"):
                key = row.get(key_field, "").strip()
                if key and key != "-":
                    index[key] = enrich
    return index


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "files",
        nargs="*",
        help="Metadata files (globs allowed): *.solo_qc.tsv and *.accessions.tsv",
    )
    parser.add_argument("--qc-suffix", default="solo_qc", help="QC file suffix (default: solo_qc)")
    parser.add_argument("--out-csv", required=True, help="Output CSV path")
    parser.add_argument("--out-json", required=True, help="Output JSON path")
    args = parser.parse_args()

    # Expand any globs the shell did not (e.g. quoted args) and classify inputs.
    paths = []
    for pattern in args.files:
        expanded = glob.glob(pattern)
        paths.extend(expanded if expanded else [pattern])

    qc_suffix = args.qc_suffix
    qc_paths = [p for p in paths if p.endswith("{}.tsv".format(qc_suffix))]
    acc_paths = [p for p in paths if p.endswith(".accessions.tsv")]

    if not qc_paths:
        sys.stderr.write(
            "WARNING: no '*.{}.tsv' QC file found among inputs; "
            "output will be based on accession data only\n".format(qc_suffix)
        )

    samples = collect_qc(qc_paths)
    accessions = collect_accessions(acc_paths)

    # Enrich QC-derived samples with matching accession metadata.
    for name, record in samples.items():
        enrich = accessions.get(name)
        if enrich:
            for field, value in enrich.items():
                record.setdefault(field, value)

    records = list(samples.values())

    # Union of all keys across records, preserving first-seen order, so the CSV
    # header covers every field even when some samples lack some columns.
    fieldnames = []
    for record in records:
        for key in record:
            if key not in fieldnames:
                fieldnames.append(key)
    if "sample_id" in fieldnames:
        fieldnames.remove("sample_id")
        fieldnames.insert(0, "sample_id")

    with open(args.out_csv, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, restval="")
        writer.writeheader()
        writer.writerows(records)

    with open(args.out_json, "w") as fh:
        json.dump(records, fh, indent=2)
        fh.write("\n")

    sys.stderr.write(
        "Aggregated {} sample record(s) from {} QC and {} accession file(s)\n".format(
            len(records), len(qc_paths), len(acc_paths)
        )
    )


if __name__ == "__main__":
    main()
