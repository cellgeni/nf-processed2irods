# nf-processed2irods

A Nextflow pipeline that takes locally reprocessed single-cell (10x/STARsolo) samples, validates them, enriches them with public metadata, runs QC, and uploads the data — with attached metadata — to [iRODS](https://irods.org/) for long-term storage.

It is the companion "upload" step to [nf-reprocessing-public-10x](https://github.com/cellgeni/nf-reprocessing-public-10x): that pipeline produces the STARsolo output locally, this one stores it on iRODS.

## Overview

Given a CSV of samples on local disk, the pipeline:

1. **Validates** each sample directory locally (`REPROCESS10X_VALIDATELOCAL`).
2. **Lists iRODS** to find what already exists, and aborts if any requested sample is already uploaded (`IRODS_LISTCOLLECTION`).
3. **Fetches public metadata** for public datasets — GEO (`GSE*`), ArrayExpress (`E-MTAB-*`), or ENA/SRA BioProjects (`PRJ*`) (`FETCH10XMETA`).
4. **Downloads barcodes/logs** for samples already present on iRODS so they can be QC'd alongside the new ones (`REPROCESS10X_IRODSBARCODESANDLOGS`).
5. **Runs STARsolo QC** and collects mapping statistics (`STARSOLOQC`).
6. **Aggregates metadata** into one per-sample CSV and JSON per dataset (`REPROCESS10X_AGGREGATEMETA`).
7. **Uploads** each sample's files to iRODS with MD5 verification (`IRODS_STOREFILE`).
8. **Attaches metadata** to the iRODS sample and dataset collections (`IRODS_ATTACHCOLLECTIONMETA`).
9. **Validates the uploaded collections** on iRODS (`REPROCESS10X_VALIDATEIRODS`).

## Quick start

```bash
# Ensure your iRODS session is initialised first
iinit

nextflow run main.nf --samples examples/samples.csv -resume
```

See `nextflow run main.nf --help` for the full option list.

There is a ready-made launcher in [`examples/RESUME`](examples/RESUME) — edit the `samples` path and run it.

## Input

### `--samples` — samples to process and upload

CSV with a header and three columns:

```csv
id,path,dataset_id
GSM5833524,/path/to/starsolo/GSE194328/GSM5833524,GSE194328
GSM5833525,/path/to/starsolo/GSE194328/GSM5833525,GSE194328
```

| Column | Description |
|---|---|
| `id` | Sample identifier (becomes the sample sub-collection name on iRODS). |
| `path` | Absolute path to the sample's STARsolo output directory. |
| `dataset_id` | Dataset the sample belongs to (groups samples under one collection). |

Each sample is uploaded to `<irodsbase>/<dataset_id>/<id>`.

### `--validatecollections` — validation-only mode

To validate collections that are **already** on iRODS (without processing or uploading anything), pass a CSV of collections instead of `--samples`:

```csv
study_accession_number,irodspath
GSE194328,/archive/cellgeni/datasets/GSE194328
GSE140393,/archive/cellgeni/datasets/GSE140393
```

`--samples` and `--validatecollections` are mutually exclusive.

## Parameters

### Input (one is required)

| Parameter | Default | Description |
|---|---|---|
| `--samples` | `null` | CSV of samples to process and upload (`id,path,dataset_id`). |
| `--validatecollections` | `null` | CSV of already-uploaded collections to validate (`study_accession_number,irodspath`). Validation-only mode. |

### iRODS

| Parameter | Default | Description |
|---|---|---|
| `--irodsbase` | `/archive/cellgeni/datasets` | Base iRODS collection for uploads. |
| `--irodsconfig` | `$HOME/.irods/irods_environment.json` | iRODS environment file used to connect. |

### Run modes

| Parameter | Default | Description |
|---|---|---|
| `--validate_local_only` | `false` | Run only local validation, then stop. |
| `--collect_metadata` | `false` | Fetch and aggregate metadata but skip the iRODS upload. |

### Processing / output

| Parameter | Default | Description |
|---|---|---|
| `--ignore_pattern` | `.bam,.bai,.cram,.crai,.fastq.gz,.fq.gz,.fastq,.fq,.sh,.bsub,.pl` | Comma-separated file patterns to skip during upload. |
| `--qc_suffix` | `solo_qc` | Suffix of the STARsolo QC files. |
| `--no_contamination_check` | `false` | Skip the STARsolo contamination check. |
| `--no_exit_local` | `false` | Do not exit on local validation errors. |
| `--outdir` | `results` | Directory for pipeline output files and reports. |
| `--help` | `false` | Show the help message and exit. |

## Outputs

Written under `--outdir` (default `results/`):

| File | Description |
|---|---|
| `mapping_qc_stats.tsv` | Per-sample STARsolo mapping QC statistics. |
| `sample_metadata.csv` | Aggregated per-sample metadata. |
| `sample_collection_metadata.csv` | Per-sample metadata plus the iRODS path it was attached to. |
| `dataset_collection_metadata.csv` | Per-dataset iRODS collection paths. |
| `md5sums.tsv` | Local vs. iRODS MD5 checksums for every uploaded file. |
| `localreports.txt` | Local validation reports. |
| `irodsreports.txt` | iRODS collection validation reports — **check this for errors/warnings.** |
| `versions.yml` | Software versions used. |

On iRODS, data is organised as:

```
<irodsbase>/
└── <dataset_id>/            # dataset collection (metadata attached)
    ├── <sample_id>/         # sample collection (metadata attached)
    │   └── [STARsolo output files]
    └── ...
```

## Requirements

- **Nextflow** ≥ 26.04.1
- **Singularity** for containerised execution
- **LSF** for job scheduling (Sanger HPC)
- An initialised **iRODS** session (`iinit`) with `~/.irods/irods_environment.json`
- Access to the `/lustre` and `/nfs` filesystems

The pipeline is configured for Sanger's HPC: an LSF executor, Singularity images from `/nfs/cellgeni/singularity/images/`, and bind mounts for `/lustre`, `/nfs` and `/etc/ssl`.

## Repository layout

| Path | Description |
|---|---|
| `main.nf` | Main workflow: validation, help, and orchestration of all steps. |
| `nextflow.config` | Sanger HPC / Singularity configuration and default parameters. |
| `configs/` | Per-process resource and argument configuration. |
| `modules/local/reprocess10x/` | Local modules: `validatelocal`, `irodsbarcodesandlogs`, `aggregatemeta`, `validateirods`. |
| `modules/local/irods/` | Local iRODS modules: `storefile`, `attachcollectionmeta`. |
| `modules/cellgeni/` | Registry modules: `irods/listcollection`, `fetch10xmeta`, `starsoloqc`. |
| `examples/` | Example input CSVs and the `RESUME` launcher script. |

## Monitoring

Execution reports are written to `reports/` with a timestamped suffix:

- `reports/execution_timeline_<timestamp>.html`
- `reports/execution_report_<timestamp>.html`
- `reports/execution_trace_<timestamp>.txt`
- `reports/pipeline_dag_<timestamp>.html`

## Troubleshooting

- **`--irodsconfig ... does not exist`** — run `iinit` first, or pass `--irodsconfig /path/to/irods_environment.json`.
- **"sample(s) already exist on iRODS"** — the pipeline refuses to overwrite existing sample collections; remove them from iRODS or drop them from your `--samples` CSV.
- **iRODS validation warnings** — inspect `results/irodsreports.txt`.
- **Detailed process logs** — see the Nextflow work directory (`nf-work/`).

## Version

Current version: 0.0.2

## License

See the [LICENSE](LICENSE) file.
