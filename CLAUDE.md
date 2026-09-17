# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this pipeline is

`nf-processed2irods` is a Nextflow (DSL2) pipeline that takes locally reprocessed 10x/STARsolo
samples, validates them, enriches them with public metadata (GEO/ArrayExpress/ENA), runs STARsolo
QC, uploads them to iRODS with MD5 verification, attaches collection metadata, and validates the
uploaded collections. It is the "upload" companion to
[nf-reprocessing-public-10x](https://github.com/cellgeni/nf-reprocessing-public-10x).

`README.md` documents all parameters, the input CSV formats, and the output files — read it before
changing user-facing behaviour, and keep it in sync with `helpMessage()` in `main.nf` and the
`params` block in `nextflow.config` (these three duplicate the parameter list).

## Commands

There is **no pipeline-level test suite, linter, or CI**. Verification is done by running the
pipeline itself, in the incremental sequence described in `README.md`. The only tests in the tree
are the nf-test suite vendored with the registry module `modules/cellgeni/fetch10xmeta/tests/`.
Those tests query GEO/SRA/ENA/BioStudies live, so they need outbound network access, and their
snapshots belong to the upstream module rather than to this pipeline — there is no `nf-test.config`
at the repo root.

```bash
module load nextflow-26.04.6           # Sanger HPC; >=26.04.6 is required (see below)
iinit                                  # initialise the iRODS session first

# Pass 1 — local validation only, no iRODS contact
nextflow run main.nf --samples examples/samples.csv --validate_local_only --no_exit_local

# Pass 2 — metadata collection, no upload
nextflow run main.nf --samples examples/samples.csv --collect_metadata -resume

# Pass 3 — full run: upload, attach metadata, validate collections
nextflow run main.nf --samples examples/samples.csv -resume

# Validation-only mode for collections already on iRODS
nextflow run main.nf --validatecollections examples/dataset_collection_metadata.csv -resume

nextflow run main.nf --help
```

Long runs are submitted to LSF rather than run on the head node — see `examples/SUBMIT.bsub`
(`bsub < examples/SUBMIT.bsub`) and `examples/RESUME`. Logs land in `logs/`, reports in `reports/`.

`-stub-run` is only a partial dry run: `IRODS_STOREFILE`, `IRODS_ATTACHCOLLECTIONMETA` and
`REPROCESS10X_AGGREGATEMETA` define `stub:` blocks, the other processes do not.

Nextflow `>=26.04.6` is pinned in the manifest because earlier 26.04.x releases mis-resolve typed
process outputs (`txt: Path = file("${id}.txt")`) when tasks are submitted as LSF array jobs.

## Architecture

### Workflow shape

`main.nf` holds the entire workflow — there are no subworkflows. Its structure is a linear
sequence of numbered steps (`STEP 0` … `STEP 6`) guarded by run-mode flags:

- `--validate_local_only` stops after STEP 1.1.
- `--collect_metadata` runs through STEP 4 but skips upload (STEP 5) and iRODS validation.
- `--validatecollections` bypasses STEPs 0–5 entirely and only runs STEP 6.
- `--samples` and `--validatecollections` are mutually exclusive; parameter validation at the top
  of the workflow calls `System.exit(1)` on conflicting combinations.

Everything downstream is driven by two channel shapes: **samples** `(meta, path)` with
`meta = [id, dataset_id]`, and **datasets** — samples grouped by `dataset_id` into
`([id: dataset_id, samples: [...]], [paths])`. Most iRODS and metadata work happens per dataset,
uploads happen per file.

### Two module conventions coexist

Older modules use the classic `tuple val(meta), path(x)` / `emit:` convention. Newer validation
modules (`REPROCESS10X_VALIDATELOCAL`, `REPROCESS10X_VALIDATEIRODS`) set
`nextflow.enable.types = true` and use static types: flat inputs (`tuple(id: String, path: Path)`)
and one output per channel, where `out.meta` carries an `ArrayTuple` record and `out.txt`/`out.list`
carry bare `Path`s with no metadata attached.

The `toTypedDataset` / `toTypedCollection` / `fromTypedMeta` / `fromTypedReport` helpers in
`main.nf` are the **only** translation layer between the two conventions. When migrating another
module to static types, adapt it there rather than reshaping the rest of the workflow.
`fromTypedReport` re-joins reports on the report file's basename (not emission order), and
`fromTypedMeta` silently drops datasets whose validation task never emitted.

### Key invariants and failure behaviours

- **One dataset directory per dataset.** `toTypedDataset` derives the dataset directory from the
  common parent of a dataset's sample paths and `error()`s if they do not share one, because
  `validate-hierarchy local` validates a whole dataset directory rather than individual samples.
- **Never overwrite iRODS.** `IRODS_LISTCOLLECTION` lists each dataset collection on the executor
  (not via blocking `ils` on the head node); samples already present are written to
  `results/already_on_irods.csv` and the run aborts. Datasets whose collection does not exist yet
  produce an empty listing, which is why the listing is re-joined with `remainder: true`.
- **Already-uploaded samples still participate in QC.** `REPROCESS10X_IRODSBARCODESANDLOGS` pulls
  their barcodes/logs back from iRODS so `STARSOLOQC` sees the whole dataset, and their accessions
  are added to the `FETCH10XMETA` sample list.
- **Upload is MD5-verified.** `IRODS_STOREFILE` computes a local md5, `iput`s with the md5 as
  metadata, then compares `ichksum` output and exits 1 on mismatch. Its `errorStrategy` retries
  with exponential backoff.
- **`--ignore_pattern`** filtering happens in `main.nf` (`ignoreExt`) by substring match on the
  file name, not inside the upload process.
- Aggregate results are assembled with `collectFile(storeDir: params.outdir)` plus `subscribe`
  logging, separately from the `publish:` / `output {}` block, which publishes reports and metadata
  with index CSVs under `outputDir`.

### Configuration layout

`nextflow.config` holds global params, the LSF executor, Singularity settings (bind mounts for
`/lustre`, `/nfs`, `/etc/ssl`; image cache `/nfs/cellgeni/singularity/images/`) and the manifest.
Per-process resources and `ext.args` live in `configs/modules/<process_name>.config`, each
explicitly `includeConfig`'d from `nextflow.config`.

Some module directories also contain a `module.config` — **these are not loaded**, and at least one
(`modules/local/reprocess10x/validateirods/module.config`) has a stale `withName` selector. Change
process resources in `configs/modules/` only, and add a new `includeConfig` line when adding a
process.

iRODS-facing processes go to the LSF `transfer` queue with capped `maxForks` and `array 100`
batching. `IRODS_STOREFILE`, `IRODS_ATTACHCOLLECTIONMETA` and
`REPROCESS10X_IRODSBARCODESANDLOGS` declare no container and rely on host i-commands
(`module load cellgen/irods`), unlike the containerised processes.

### Validation schemas

Local and iRODS validation are driven by YAML hierarchy schemas in `configs/schema/`, consumed by
the `validate-hierarchy` CLI in the `quay.io/cellgeni/track-reprocessing` container.

The schemas the pipeline actually uses are the `*.anchored.yml` pair — `local_dataset_root.anchored.yml`
(`--schema_local`) and `dataset_root.anchored.yml` (`--schema_irods`) — self-contained plain YAML
using anchors/aliases. The `dataset_root.yml` / `local_dataset_root.yml` + `_*.yml` fragment files
are the older `!include`-based variants kept alongside them; edit the anchored files unless you are
deliberately reviving the include loader.

The two anchored schemas are intentionally near-identical: the local one makes the dataset-level
metadata files (`ena`/`sra`, `parsed`, `solo_qc`) optional because `FETCH10XMETA`/`STARSOLOQC`
produce them later in the pipeline. A schema change to file or collection patterns usually needs
applying to both.

### Modules from the registry

`modules/cellgeni/**` (`irods/listcollection`, `fetch10xmeta`, `starsoloqc`) are pulled from the
Nextflow module registry (`registry.nextflow.io`) and carry a `.module-info` with a checksum. Do
not hand-edit them — change them upstream and re-install. Pipeline-specific code belongs in
`modules/local/**`.

`fetch10xmeta` ships its own helper scripts in `resources/usr/bin/`, which reach the task `PATH`
through `nextflow.enable.moduleBinaries = true` in `nextflow.config`; every other process invokes
tools baked into its container (`quay.io/cellgeni/track-reprocessing`,
`quay.io/cellgeni/starsolo`, `community.wave.seqera.io/.../python-irodsclient`).

## Working in this checkout

`.gitignore` excludes the runtime and scratch directories: `nf-work/`, `results/`, `reports/`,
`logs/`, `.nextflow*`, `.lineage/`, `data/`, `scripts/`, `irodsvalidation/` and `*.log`. `examples/`
is tracked; `data/tables/` and `scripts/` hold the maintainer's real run inputs and are not. Do not
assume files under those paths are part of the repository.
