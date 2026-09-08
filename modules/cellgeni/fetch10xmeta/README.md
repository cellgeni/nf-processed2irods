# cellgeni/fetch10xmeta

## Summary

Fetches and parses metadata for public 10x datasets from GEO (`GSE*`), ArrayExpress (`E-MTAB*`),
or BioProject/ENA (`PRJ*`). For each dataset it:

1. Downloads raw metadata from NCBI SRA, EBI ENA, or BioStudies depending on accession type.
2. Resolves sample accessions to experiment and run IDs, building an accessions map.
3. Classifies each run by download type (paired-end FASTQs, 10x BAM, or SRA) and assigns its species via `parse_metadata.sh`, which reads the ENA and SRA tables together — a run is often listed in only one of them, and the species recorded in the other is what keeps it from being reported as `UNKNOWN`. ENA is preferred wherever both have an answer, and the NCBI SDL API is queried at most once per run.
4. Merges the per-run classification with sample IDs into `links.tsv` via `add_samples.awk`.

For GEO datasets the module falls back through project IDs → sub-series project IDs → BioSample IDs if earlier ENA/SRA metadata downloads fail.

## Inputs

| Name | Type | Description |
|---|---|---|
| `meta.id` | string | Dataset accession. Supported prefixes: `GSE*` (GEO), `E-MTAB*` (ArrayExpress), `PRJ*` (BioProject). |
| `sample_ids` | string | Comma-separated sample accessions to restrict processing to (e.g. `GSM7232572,GSM7232573`, `ERS4689152,ERS4689153`). Pass empty/null to process all samples in the dataset. |

## Outputs

| Name | File(s) | Description |
|---|---|---|
| `links` | `links.tsv` | Per-run metadata with an appended `sample_id` column mapping each run back to its source sample. |
| `list` | `*.list` | Accession list files: run list (`*.run.list`), sample list (`*.sample.list`), project list (`*.project.list`), etc. |
| `tsv` | `*.tsv` | TSV files from the collection pipeline: raw SRA/ENA metadata, accession mapping (`*.accessions.tsv`), sample-run mapping (`*.sample_x_run.tsv`), and parsed run classification (`*.parsed.tsv`). |
| `txt` | `*.txt` | Optional SDRF/IDF plain-text files, present for ArrayExpress (`E-MTAB*`) datasets. |
| `soft` | `*_family.soft` | Optional GEO SOFT family file, present for GEO (`GSE*`) datasets. |
| `versions` | `versions.yml` | Pipeline version record. |

## Usage

```nextflow
include { FETCH10XMETA } from 'cellgeni/fetch10xmeta'

// GEO dataset with comma-separated sample IDs
FETCH10XMETA(
    channel.of([[id: 'GSE230685'], 'GSM7232572,GSM7232573'])
)

// All samples in an ArrayExpress dataset (no sample ID filter)
FETCH10XMETA(
    channel.of([[id: 'E-MTAB-9221'], null])
)
```

## Tests

`tests/main.nf.test` runs the module against fourteen real datasets, chosen to cover
every accession type and every download route the module can pick. See
[the repository README](../../../README.md#tests) for how to run them.

| Test | Dataset | Covers |
|---|---|---|
| ENA paired-end fastq | `GSE111360` | `ENAFQ`, several runs per sample |
| ENA paired-end fastq — mouse | `GSE160513` | species other than human |
| one run per sample | `GSE250130` | run count must equal sample count |
| mixed ENA fastq and SRA | `GSE264508` | a series that needs both routes at once |
| SRA archive only | `GSE117988` | `SRA` |
| 10x BAM | `GSE274955` | `BAM` |
| ArrayExpress SDRF | `E-MTAB-9221` | `ORIFQ`, ERS samples, `*.txt` output |
| BioProject | `PRJNA511433` | SRS samples, no SOFT file |
| family file without SRA relations | `GSE135325`, `GSE137444` | see below |
| sample subsets | `GSE135325`, `GSE117988`, `E-MTAB-9221` | nothing outside the subset leaks through |
| stub | `GSE111360` | the `stub:` block |

The `relation-recovery` tests are regression tests for GEO series whose
`family.soft` records `!Sample_relation = BioSample:` but no `!Sample_relation =
SRA:` line. Those used to leave the sample list empty and fail the process with
`No run list '<series>.run.list' found!`; the relations are now recovered from
the SRA/ENA metadata tables instead.

### What is asserted

Snapshots cover the run, species, type and sample columns of `links.tsv`. The
download URL column is deliberately left out: SDL and the SRA mirrors hand out
URLs whose host, path and (for BAMs) signature change between calls, so
snapshotting them would break the suite within days. It is checked for shape
instead, alongside the sample accessions, the species (never `UNKNOWN`) and the
download types each series is expected to resolve to.

`links.tsv` has no header, so the [nft-csv](https://github.com/lukfor/nft-csv)
plugin names its columns `C0`–`C4`:

| Column | Contents |
|---|---|
| `C0` | run accession |
| `C1` | species |
| `C2` | download URL(s) — not snapshotted |
| `C3` | download type: `ORIFQ`, `ENAFQ`, `BAM` or `SRA` |
| `C4` | sample accession |

## License

MIT
