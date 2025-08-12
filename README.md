# nf-processed2irods

A Nextflow pipeline to store processed single-cell genomics data and metadata in iRODS (Integrated Rule-Oriented Data System).

## Overview

This pipeline processes datasets containing single-cell RNA sequencing results (typically from STARsolo/CellRanger), extracts metadata, performs quality control analysis, and uploads the data with comprehensive metadata to iRODS for long-term storage and data management.

## Contents of Repo:
* `main.nf` — the main Nextflow pipeline that orchestrates data processing and iRODS upload
* `nextflow.config` — configuration script for IBM LSF submission on Sanger's HPC with Singularity containers and global parameters
* `modules/local/reprocess10x/parsemetadata/` — module for parsing metadata from public repositories (GEO, ENA)
* `modules/local/reprocess10x/mappingqc/` — module for extracting mapping quality control statistics
* `modules/local/irods/storefile/` — module for uploading files to iRODS
* `modules/local/irods/attachcollectionmeta/` — module for attaching metadata to iRODS collections

## Pipeline Workflow

1. **Dataset Discovery**: Reads dataset information from CSV input file
2. **Public Dataset Detection**: Identifies public datasets (GSE*, E-MTAB-*, PRJEB* patterns) 
3. **Metadata Parsing**: Extracts metadata from public repositories for public datasets
4. **Quality Control**: Generates mapping QC statistics from STARsolo output (if not already present)
5. **File Collection**: Gathers all data files and metadata files for upload
6. **iRODS Upload**: Transfers files to iRODS with checksums
7. **Metadata Attachment**: Attaches comprehensive metadata to iRODS collections

## Examples

### Basic Usage
Upload processed datasets to iRODS:
```bash
nextflow run main.nf \
    --datasets datasets.csv \
    --irodspath "/archive/cellgeni/sanger/"
```

### With Unmapped Reads Removal
Remove large unmapped read files to save storage space:
```bash
nextflow run main.nf \
    --datasets datasets.csv \
    --irodspath "/archive/cellgeni/sanger/" \
    --remove_unmapped_reads true
```

### Keep Unmapped Reads
Preserve all files including unmapped reads:
```bash
nextflow run main.nf \
    --datasets datasets.csv \
    --irodspath "/archive/cellgeni/sanger/" \
    --remove_unmapped_reads false
```

### Custom Output Directory
Specify a different output directory:
```bash
nextflow run main.nf \
    --datasets datasets.csv \
    --irodspath "/archive/cellgeni/sanger/" \
    --output_dir "my_results"
```

## Pipeline Parameters

### Required Parameters:
* `--datasets` — Path to a CSV file containing dataset information with columns: `id` (dataset identifier) and `path` (local filesystem path to processed data directory)
* `--irodspath` — Base path in iRODS where datasets will be stored (e.g., "/archive/cellgeni/sanger/")

### Optional Parameters:
* `--remove_unmapped_reads` — Remove unmapped read files (*.Unmapped.out.mate*.bz2) to save storage space (`default: false`)
* `--output_dir` — Output directory for pipeline results (`default: "results"`)
* `--publish_mode` — File publishing mode (`default: "copy"`)

## Input File Format

The `--datasets` parameter expects a CSV file with the following structure:

```csv
id,path
GSE123456,/path/to/processed/GSE123456
PRJEB12345,/path/to/processed/PRJEB12345
EGA_DATASET,/path/to/processed/EGA_DATASET
```

Where:
- `id`: Unique dataset identifier (can be GEO accession, ENA project, or custom ID)
- `path`: Absolute path to the directory containing processed single-cell data

## Expected Data Structure

Each dataset directory should contain:
- **Sample directories**: Named with sample identifiers containing STARsolo output files
- **QC files**: `*solo_qc.tsv` files (generated automatically if missing)
- **Metadata files**: For public datasets, metadata will be automatically retrieved

Example directory structure:
```
GSE123456/
├── SAMPLE1/
│   ├── Aligned.sortedByCoord.out.bam
│   ├── Log.final.out
│   ├── Solo.out/
│   └── ...
├── SAMPLE2/
│   └── ...
└── GSE123456_solo_qc.tsv
```

## Metadata Handling

### Public Datasets
For datasets matching patterns `GSE*`, `E-MTAB-*`, or `PRJEB*`, the pipeline automatically:
- Retrieves sample metadata from public repositories
- Generates accession mapping files
- Creates sample relationship files
- Downloads study metadata

### All Datasets
The pipeline extracts and stores:
- **Sample-level metadata**: Species, sequencing type (paired/single), strand information, read counts, whitelist version
- **Dataset-level metadata**: Study accession numbers, dataset identifiers
- **Technical metadata**: File checksums, upload timestamps

## iRODS Structure

Data is organized in iRODS as:
```
/archive/cellgeni/sanger/
├── GSE123456/
│   ├── SAMPLE1/
│   │   ├── [STARsolo output files]
│   │   └── [metadata attached to collection]
│   ├── SAMPLE2/
│   └── [dataset metadata files]
└── PRJEB12345/
    └── ...
```

## System Requirements

- **Nextflow**: Version 25.04.4 or higher
- **Singularity**: For containerized execution
- **LSF**: For job scheduling on HPC
- **iRODS**: Client tools for data upload
- **Storage**: Sufficient space in `/lustre` and `/nfs` mount points

## Configuration

The pipeline is configured for Sanger's HPC environment with:
- LSF executor with per-job memory limits
- Singularity containers from `/nfs/cellgeni/singularity/images/`
- Bind mounts for `/lustre` and `/nfs` filesystems
- Retry strategy with up to 5 attempts per process

## Monitoring

The pipeline generates comprehensive reports:
- Timeline report: `reports/YYYYMMDD-HH-mm-ss_timeline.html`
- Execution report: `reports/YYYYMMDD-HH-mm-ss_report.html`
- Trace file: `reports/YYYYMMDD-HH-mm-ss_trace.tsv`

## Pipeline Outputs

- **iRODS Collections**: Organized dataset collections with attached metadata
- **QC Reports**: Mapping statistics and quality metrics
- **Metadata Files**: Comprehensive sample and dataset annotations
- **Upload Logs**: Records of transferred files with checksums

## Troubleshooting

### Common Issues:
1. **Missing QC files**: The pipeline will generate them automatically
2. **iRODS connection**: Ensure iRODS client is properly configured
3. **File permissions**: Check read access to input directories
4. **Storage space**: Ensure sufficient space for temporary files

### Log Files:
Check Nextflow work directory (`nf-work/`) for detailed process logs and error messages.

## Version

Current version: 0.0.1

## License

See repository for license information.
