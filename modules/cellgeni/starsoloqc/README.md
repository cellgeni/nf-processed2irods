# cellgeni/starsoloqc

## Summary

Collects QC statistics from STARsolo output directories, producing a merged TSV summary
of alignment and cell-calling metrics across all samples in a dataset.

The module runs `starsolo qc` over all per-sample subdirectories (`*/`) and writes the
combined output to a single `{id}.solo_qc.tsv` file.

## Inputs

| Name | Type | Description |
|---|---|---|
| `meta.id` | string | Dataset accession used as the output file prefix (e.g. `GSE230685`). |
| `samples` | directories | STARsolo per-sample output directories to collect QC metrics from. |

## Outputs

| Name | File(s) | Description |
|---|---|---|
| `tsv` | `*.solo_qc.tsv` | Merged QC statistics across all STARsolo sample directories. |
| `versions` | `versions.yml` | STARsolo wrapper version record. |

## Usage

```nextflow
include { STARSOLOQC } from 'cellgeni/starsoloqc'

STARSOLOQC(
    channel.of([[id: 'GSE230685'], file('starsolo_outputs/')])
)
```

## License

MIT
