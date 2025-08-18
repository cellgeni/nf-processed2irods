# nf-processed2i## Pipeline Workflow

1. **Dataset/Sample Discovery** - Reads dataset or sample information from CSV input file
2. **Public Dataset Detection** - Identifies public datasets (GSE*, E-MTAB-*, PRJEB* patterns) 
3. **Metadata Parsing** - Extracts metadata from public repositories for public datasets
4. **Quality Control** - Generates mapping QC statistics from STARsolo output (if not already present)
5. **File Collection** - Gathers all data files and metadata files for upload
6. **iRODS Upload** - Transfers files to iRODS with checksums
7. **Metadata Attachment** - Attaches comprehensive metadata to iRODS collections

## Dry Run Mode

The pipeline supports a dry run mode (`--dry_run true`) that allows you to test and validate your pipeline configuration without actually uploading data to iRODS. This is particularly useful for:

### What Dry Run Does:
- **Validates input parameters** - Checks that all required parameters are provided and correctly formatted
- **Processes metadata** - Runs all metadata collection and parsing steps
- **Generates QC reports** - Creates mapping statistics and quality control files
- **Creates output files** - Generates `sample_metadata.csv` and `dataset_metadata.csv` files locally
- **Shows intended operations** - Displays what files would be uploaded and where

### What Dry Run Skips:
- **iRODS file uploads** - No files are actually transferred to iRODS storage
- **iRODS metadata attachment** - No metadata is attached to iRODS collections
- **MD5 checksum validation** - No checksums are computed or compared with iRODS

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

### Basic Usage with Datasets
Upload processed datasets to iRODS:
```bash
nextflow run main.nf \
    --datasets datasets.csv \
    --irodspath "/archive/cellgeni/sanger/"
```

### Basic Usage with Individual Samples
Upload individual samples to iRODS:
```bash
nextflow run main.nf \
    --samples samples.csv \
    --irodspath "/archive/cellgeni/sanger/"
```

### Custom File Filtering
Specify which file types to ignore during upload:
```bash
nextflow run main.nf \
    --datasets datasets.csv \
    --irodspath "/archive/cellgeni/sanger/" \
    --ignore_pattern ".bam,.fastq.gz"
```

### Disable Public Metadata Collection
Skip automatic metadata retrieval for public datasets:
```bash
nextflow run main.nf \
    --datasets datasets.csv \
    --irodspath "/archive/cellgeni/sanger/" \
    --collect_public_metadata false
```

### Enable Verbose Output
Get detailed logging information:
```bash
nextflow run main.nf \
    --datasets datasets.csv \
    --irodspath "/archive/cellgeni/sanger/" \
    --verbose true
```

### Custom Output Directory
Specify a different output directory:
```bash
nextflow run main.nf \
    --datasets datasets.csv \
    --irodspath "/archive/cellgeni/sanger/" \
    --output_dir "my_results"
```

### Dry Run (Test Mode)
Test the pipeline without actually uploading files to iRODS:
```bash
nextflow run main.nf \
    --datasets datasets.csv \
    --irodspath "/archive/cellgeni/sanger/" \
    --dry_run true
```

This mode will:
- Validate all input parameters and files
- Process metadata and generate QC reports
- Show what would be uploaded without actual iRODS operations
- Create local output files (metadata CSV files) for review

## Pipeline Parameters

### Required Parameters:
* `--datasets` — Path to a CSV file containing dataset information with columns: `id` (dataset identifier) and `path` (local filesystem path to processed data directory)
  **OR**
* `--samples` — Path to a CSV file containing sample information with columns: `id` (sample identifier), `path` (local filesystem path), and optionally `dataset_id`
* `--irodspath` — Base path in iRODS where datasets will be stored (e.g., "/archive/cellgeni/sanger/")

### Optional Parameters:
* `--output_dir` — Output directory for pipeline results (`default: "results"`)
* `--publish_mode` — File publishing mode (`default: "copy"`)
* `--ignore_pattern` — Comma-separated list of file patterns to ignore during upload (`default: ".bam,.bai,.cram,.crai,.fastq.gz,.fq.gz,.fastq,.fq,.mate1.bz2,.mate2.bz2,.sh,.bsub,.pl"`)
* `--collect_public_metadata` — Collect metadata from public repositories for public datasets (`default: true`)
* `--parse_mapper_metrics` — Parse mapping QC metrics from STARsolo output (`default: true`)
* `--verbose` — Enable verbose output (`default: false`)
* `--dry_run` — Perform a dry run without uploading files to iRODS (`default: false`)

## Input File Format

The pipeline supports two input modes:

### Option 1: Dataset-based input (`--datasets`)
CSV file with the following structure:

```csv
id,path
GSE123456,/path/to/processed/GSE123456
PRJEB12345,/path/to/processed/PRJEB12345
EGA_DATASET,/path/to/processed/EGA_DATASET
```

Where:
- `id`: Unique dataset identifier (can be GEO accession, ENA project, or custom ID)
- `path`: Absolute path to the directory containing processed single-cell data with sample subdirectories

### Option 2: Sample-based input (`--samples`)
CSV file with the following structure:

```csv
id,path,dataset_id
SAMPLE1,/path/to/processed/SAMPLE1,GSE123456
SAMPLE2,/path/to/processed/SAMPLE2,GSE123456
SAMPLE3,/path/to/processed/SAMPLE3,
```

Where:
- `id`: Unique sample identifier 
- `path`: Absolute path to the sample directory containing processed single-cell data
- `dataset_id`: (Optional) Dataset identifier to group samples under. If omitted, samples will be uploaded directly to irodspath/id

## iRODS Path Structure

The pipeline uploads data to different iRODS paths depending on the input type:

- **Datasets (`--datasets`)**: Uploaded to `irodspath/id`
  - Example: `/archive/cellgeni/sanger/GSE123456/`
  
- **Samples with dataset_id (`--samples`)**: Uploaded to `irodspath/dataset_id/id`
  - Example: `/archive/cellgeni/sanger/GSE123456/SAMPLE1/`
  
- **Samples without dataset_id (`--samples`)**: Uploaded to `irodspath/id`
  - Example: `/archive/cellgeni/sanger/SAMPLE1/`

## Expected Data Structure

### For Dataset-based Input
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

### For Sample-based Input
Each sample directory should contain STARsolo output files:
```
SAMPLE1/
├── Aligned.sortedByCoord.out.bam
├── Log.final.out
├── Solo.out/
│   ├── Gene/
│   ├── GeneFull/
│   └── ...
└── ...
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

Data is organized in iRODS based on the input type:

### For Dataset-based Input (`--datasets`)
```
/archive/cellgeni/sanger/
├── GSE123456/                    # Dataset uploaded to irodspath/id
│   ├── SAMPLE1/                  # Individual samples within dataset
│   │   ├── [STARsolo output files]
│   │   └── [metadata attached to collection]
│   ├── SAMPLE2/
│   └── [dataset metadata files]
└── PRJEB12345/                   # Another dataset
    └── ...
```

### For Sample-based Input (`--samples`)
**With dataset_id specified:**
```
/archive/cellgeni/sanger/
├── GSE123456/                    # Dataset ID from CSV
│   ├── SAMPLE1/                  # Sample uploaded to irodspath/dataset_id/id
│   │   ├── [STARsolo output files]
│   │   └── [metadata attached to collection]
│   └── SAMPLE2/
└── ...
```

**Without dataset_id specified:**
```
/archive/cellgeni/sanger/
├── SAMPLE1/                      # Sample uploaded directly to irodspath/id
│   ├── [STARsolo output files]
│   └── [metadata attached to collection]
├── SAMPLE2/
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
