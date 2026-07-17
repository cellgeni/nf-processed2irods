# cellgeni/fetch10xmeta

## Summary

Fetches and parses metadata for public 10x datasets from GEO (`GSE*`), ArrayExpress (`E-MTAB*`),
or BioProject/ENA (`PRJ*`). For each dataset it:

1. Downloads raw metadata from NCBI SRA, EBI ENA, or BioStudies depending on accession type.
2. Resolves sample accessions to experiment and run IDs, building an accessions map.
3. Classifies each run by download type (paired-end FASTQs, 10x BAM, or SRA) via `parse_ena_metadata.sh` / `parse_sra_metadata.sh`.
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

## License

MIT
