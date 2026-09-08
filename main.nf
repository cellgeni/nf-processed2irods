include { REPROCESS10X_VALIDATELOCAL } from './modules/local/reprocess10x/validatelocal'
include { REPROCESS10X_IRODSBARCODESANDLOGS } from './modules/local/reprocess10x/irodsbarcodesandlogs'
include { REPROCESS10X_AGGREGATEMETA } from './modules/local/reprocess10x/aggregatemeta'
include { REPROCESS10X_VALIDATEIRODS } from './modules/local/reprocess10x/validateirods'
include { IRODS_ATTACHCOLLECTIONMETA } from './modules/local/irods/attachcollectionmeta'
include { IRODS_STOREFILE } from './modules/local/irods/storefile'
include { IRODS_LISTCOLLECTION } from 'cellgeni/irods/listcollection'
include { FETCH10XMETA } from 'cellgeni/fetch10xmeta'
include { STARSOLOQC } from 'cellgeni/starsoloqc'

// HELP MESSAGE
def helpMessage() {
    log.info"""
    =============================
    Store Processed Data to iRODS
    =============================
    This pipeline validates locally reprocessed 10x samples, fetches public metadata,
    runs STARsolo QC, uploads the data to iRODS and attaches collection metadata.

    Usage: nextflow run main.nf [OPTIONS]

    Required (one of):
        --samples             Path to a CSV file of samples to process and upload.
                              Columns: id,path,dataset_id
        --validatecollections Path to a CSV file of already-uploaded collections to
                              validate. Columns: study_accession_number,irodspath
                              (validation-only mode; cannot be combined with --samples)

    Optional:
        --outdir              Directory to save results (default: results).
        --irodsbase           Base iRODS collection for uploads
                              (default: /archive/cellgeni/datasets).
        --irodsconfig         Path to irods_environment.json used to connect to iRODS
                              (default: \$HOME/.irods/irods_environment.json).
        --schema_local        Schema used to validate local dataset directories
                              (default: configs/schema/local_dataset_root.anchored.yml).
        --schema_irods        Schema used to validate uploaded iRODS collections
                              (default: configs/schema/dataset_root.anchored.yml).
        --validate_local_only Only run local validation, then stop (default: false).
        --collect_metadata    Fetch/aggregate metadata but skip the iRODS upload
                              (default: false).
        --ignore_pattern      Comma-separated file patterns to skip during upload
                              (default: ${params.ignore_pattern}).
        --qc_suffix           Suffix of the STARsolo QC files (default: solo_qc).
        --no_contamination_check  Skip the STARsolo contamination check (default: false).
        --no_exit_local       Do not exit on local validation errors (default: false).
        --help                Show this help message and exit.

    Examples:
        # Process and upload samples
        nextflow run main.nf --samples examples/samples.csv -resume

        # Validate collections already on iRODS
        nextflow run main.nf --validatecollections examples/dataset_collection_metadata.csv -resume

    == samples.csv format ==
    id,path,dataset_id
    GSM5833524,/path/to/starsolo/GSE194328/GSM5833524,GSE194328
    GSM5833525,/path/to/starsolo/GSE194328/GSM5833525,GSE194328
    ========================

    == validatecollections.csv format ==
    study_accession_number,irodspath
    GSE194328,/archive/cellgeni/datasets/GSE194328
    =====================================
    """.stripIndent()
}

def checkIfPublic(series) {
    return (series ==~ /GSE\d+/) || (series ==~ /E-MTAB-\d+/) || (series ==~ /PRJ.{0,3}\d+/)
}

def ignoreExt(path, ignore_ext) {
    return !ignore_ext.any { ext -> path.name.contains(ext) }
}

// Parse the CSV emitted by IRODS_LISTCOLLECTION (columns: type,path,size,checksum)
// and return the basenames of its immediate sub-collections.
def subcollectionNames(csv) {
    return csv.splitCsv(header: true)
        .findAll { row -> row.type == 'collection' }
        .collect { row -> row.path.replaceAll('/$', '').tokenize('/').last() }
}

/////////////// ADAPTERS FOR STATICALLY TYPED MODULES ////////////////////////
// The validation modules are written with static types: they take flat values
// (`tuple(id: String, path: Path)`) and emit every output on its own channel --
// `out.meta` carries an ArrayTuple wrapping the per-task record, while
// `out.txt`/`out.list` carry bare Paths with no metadata attached. The rest of
// this workflow still speaks the `(meta: Map, files)` convention, so the
// helpers below translate between the two. They are the only place that needs
// to change as the remaining modules are migrated to static types.

// (meta, samplepaths) -> tuple(id, dataset directory).
// A dataset is validated as a whole directory, so the sample paths of a dataset
// must share one parent directory named after the dataset.
def toTypedDataset(datasetch) {
    return datasetch.map { meta, paths ->
        def samplepaths = paths instanceof List ? paths : [paths]
        def parents = samplepaths.collect { path -> path.parent }.unique()
        if (parents.size() != 1) {
            error("Samples of dataset ${meta.id} live in more than one directory: ${parents.join(', ')}. All samples of a dataset must share one dataset directory.")
        }
        tuple(meta.id, parents.first())
    }
}

// (meta, irodspath) -> tuple(id, irodspath)
def toTypedCollection(collectionch) {
    return collectionch.map { meta, irodspath -> tuple(meta.id, irodspath.toString()) }
}

// out.meta (ArrayTuple of [id: .., path: ..]) -> the entries of `datasetch`
// that the process emitted, in the original (meta, files) shape. Tasks that
// failed validation never emit, so they are dropped here as well.
def fromTypedMeta(typedmetach, datasetch) {
    return datasetch
        .map { meta, files -> tuple(meta.id, meta, files) }
        .join(typedmetach.map { record -> tuple(record[0].id, record[0].path) })
        .map { _id, meta, files, _typedpath -> tuple(meta, files) }
}

// out.txt (bare Paths named "<id>.txt") -> (meta, report), keyed on the report
// file name so the pairing does not rely on channel emission order.
def fromTypedReport(reportch, datasetch) {
    return reportch
        .map { report -> tuple(report.baseName, report) }
        .join(datasetch.map { meta, _files -> tuple(meta.id, meta) })
        .map { _id, report, meta -> tuple(meta, report) }
}

workflow {
    main:
    /////////////// PARAMETER VALIDATION ////////////////////////
    // Show help and exit (exit 0 for --help, exit 1 when no input was given).
    if (params.help || (!params.samples && !params.validatecollections)) {
        helpMessage()
        System.exit(params.help ? 0 : 1)
    }

    // --samples and --validatecollections are mutually exclusive.
    if (params.samples && params.validatecollections) {
        log.error("--samples and --validatecollections cannot be used together. Provide only one.")
        System.exit(1)
    }

    // --validatecollections is a standalone validation mode; the processing/upload
    // flags do not apply to it.
    if (params.validatecollections && (params.validate_local_only || params.collect_metadata)) {
        log.error("--validate_local_only and --collect_metadata cannot be combined with --validatecollections.")
        System.exit(1)
    }

    // Steps that talk to iRODS need a readable irods_environment.json.
    def needsIrods = params.validatecollections || (params.samples && !params.validate_local_only)
    if (needsIrods && (!params.irodsconfig || !file(params.irodsconfig).exists())) {
        log.error("iRODS access is required but --irodsconfig '${params.irodsconfig}' does not exist. Run `iinit` or pass --irodsconfig.")
        System.exit(1)
    }

    /////////////// PARAMETER INITIALIZATION ////////////////////////
    def ignore_ext = params.ignore_pattern ? params.ignore_pattern.split(',').collect { ext -> ext.trim() }.findAll { ext -> ext } : []
    def sampleMetaColumns = [
                'sample_id', 'dataset_id', 'species', 'paired', 'strand', 'total_reads',
                'whitelist', 'cells', 'geo_sample', 'sample', 'experiment', 'run',
            ]

    /////////////// INITIALIZE CHANNELS ////////////////////////
    versions            = channel.topic('versions')
    metadata            = channel.empty()
    outmetadata         = channel.empty()
    outdatasetmeta      = channel.empty()
    outsamplemeta       = channel.empty()
    validatelocal       = channel.empty()
    validateirods       = channel.empty()
    md5sums             = channel.empty()
    samples             = params.samples && !params.validatecollections ? channel.fromPath(params.samples, checkIfExists: true) : channel.empty()
    irodsconfig         = needsIrods ? channel.value(file(params.irodsconfig, type: 'file', checkIfExists: true)) : channel.empty()
    validatecollections = params.validatecollections ? channel.fromPath(params.validatecollections, checkIfExists: true) : channel.empty()
    schema_local        = channel.value(file(params.schema_local, type: 'file', checkIfExists: true))
    schema_irods        = channel.value(file(params.schema_irods, type: 'file', checkIfExists: true))
    
    /////////////// STEP 0: INPUTS ///////////////////////
    samples = samples
        .splitCsv(header: true, sep: ',')
        .map { row -> tuple([id: row.id, dataset_id: row.dataset_id], file(row.path, type: 'dir', checkIfExists: true))}
    
    datasets = samples
        .map { meta, path -> tuple(meta.dataset_id, meta.id, path) }
        .groupTuple(sort: 'hash')
        .map { dataset_id, samplelist, paths -> tuple([id: dataset_id, samples: samplelist], paths) }
    
    validatecollections = validatecollections
        .splitCsv(header: true, sep: ',')
        .map { row -> tuple([id: row.study_accession_number, study_accession_number: row.study_accession_number], row.irodspath) }


    /////////////// STEP 1.1: VALIDATE LOCAL DIRECTORIES ///////////////
    REPROCESS10X_VALIDATELOCAL(toTypedDataset(datasets), schema_local)
    validateddatasets = fromTypedMeta(REPROCESS10X_VALIDATELOCAL.out.meta, datasets)
    validatelocal = validatelocal.mix(fromTypedReport(REPROCESS10X_VALIDATELOCAL.out.txt, datasets))

    if (!params.validate_local_only) {
        /////////////// STEP 1.2: LIST WHAT ALREADY EXISTS ON iRODS ///////////////////////
        // List each dataset's iRODS collection once, on the executor rather than
        // blocking the head node with synchronous `ils`. Datasets whose collection
        // does not exist yet exit 2 and are dropped by the process errorStrategy, so
        // we re-join the listing against the full dataset channel with an empty
        // listing as the default (remainder: true).
        IRODS_LISTCOLLECTION(
            validateddatasets.map { meta, _pathlist ->
                tuple(meta, "${params.irodsbase}/${meta.id}".toString())
            },
            irodsconfig
        )
        versions = versions.mix(IRODS_LISTCOLLECTION.out.versions.first())

        // meta.id (dataset) -> list of sample sub-collections already on iRODS
        datasetUploaded = validateddatasets
            .map { meta, _pathlist -> tuple(meta.id, meta) }
            .join(
                IRODS_LISTCOLLECTION.out.csv.map { meta, csv -> tuple(meta.id, subcollectionNames(csv)) },
                remainder: true
            )
            .map { _id, meta, uploaded -> tuple(meta, uploaded ?: []) }

        // Abort if any requested sample already exists on iRODS. The full list is
        // written to results/already_on_irods.csv (dataset_id,sample_id,irodspath)
        // so it can be inspected or fed to a cleanup script, rather than scrolling
        // past hundreds of warning lines.
        alreadyOnIrods = samples
            .map { meta, _path -> tuple(meta.dataset_id, meta) }
            .combine(
                datasetUploaded.map { meta, uploaded -> tuple(meta.id, uploaded) },
                by: 0
            )
            .filter { _dataset_id, meta, uploaded -> meta.id in uploaded }
            .map { _dataset_id, meta, _uploaded -> meta }

        alreadyOnIrods
            .collectFile(
                name: 'already_on_irods.csv',
                storeDir: params.outdir,
                newLine: true,
                sort: true,
                seed: 'dataset_id,sample_id,irodspath'
            ) { meta ->
                "${meta.dataset_id},${meta.id},${params.irodsbase}/${meta.dataset_id}/${meta.id}"
            }
            .subscribe { reportFile ->
                // collectFile only emits when at least one sample matched; the header
                // seed alone means every requested sample is new (2 lines incl. header
                // would mean one match). Count data rows and abort if any exist.
                def dataRows = reportFile.readLines().findAll { line -> line && !line.startsWith('dataset_id,') }
                if (dataRows.size() > 0) {
                    error("${dataRows.size()} sample(s) already exist on iRODS. See ${reportFile} for the full list. Exiting...")
                }
            }

        /////////////// STEP 2: FETCH METADATA ////////////////////////
        public_datasets = validateddatasets
            .filter { meta, _pathlist -> checkIfPublic(meta.id) }
            .map { meta, pathlist ->
                def paths = pathlist instanceof List ? pathlist : [pathlist] // ensure pathlist is a list
                def processedsamples = paths.findAll { it -> it.isDirectory() }.collect { it -> it.baseName }
                tuple(meta.id, meta, processedsamples)
            }
            .join(datasetUploaded.map { meta, uploaded -> tuple(meta.id, uploaded) })
            .map { _id, meta, processedsamples, uploaded ->
                tuple(meta, (processedsamples + uploaded).unique().sort().join(','))
            }
        FETCH10XMETA(public_datasets)
        versions = versions.mix(FETCH10XMETA.out.versions.first())
        metadata = metadata.mix(
            FETCH10XMETA.out.tsv,
            FETCH10XMETA.out.list,
            FETCH10XMETA.out.soft,
            FETCH10XMETA.out.txt
        )

        ////////////// STEP 3.1: DOWNLOAD BARCODES AND LOGS FOR PROCESSED SAMPLES ////////////////////////
        processedsamples = datasetUploaded
            .flatMap { meta, uploaded ->
                uploaded.collect { sample ->
                    def irodspath = "${params.irodsbase}/${meta.id}/${sample}"
                    tuple([id: sample, dataset_id: meta.id], irodspath)
                }
            }

        REPROCESS10X_IRODSBARCODESANDLOGS(processedsamples)
        versions = versions.mix(REPROCESS10X_IRODSBARCODESANDLOGS.out.versions.first())

        ////////////// STEP 3.2: RUN STARSOLO QC ////////////////////////
        groupedsamples = samples.mix(REPROCESS10X_IRODSBARCODESANDLOGS.out.sample)
            .map { meta, path -> tuple(meta.dataset_id, meta.id, path) }
            .groupTuple(sort: 'hash')
            .map { dataset_id, samplelist, paths -> tuple([id: dataset_id, samples: samplelist], paths) }
        
        STARSOLOQC(groupedsamples)
        metadata = metadata.mix(STARSOLOQC.out.tsv)
        versions = versions.mix(STARSOLOQC.out.versions.first())

        // Collect mapping QC stats
        STARSOLOQC.out.tsv
            .splitCsv(sep: '\t', skip: 1)
            .collectFile(
                name: 'mapping_qc_stats.tsv',
                storeDir: params.outdir,
                newLine: true,
                seed: "Dataset\tSample\tRd_all\tRd_in_cells\tFrc_in_cells\tUMI_in_cells\tCells\tMed_nFeature\tGood_BC\tWL\tSpecies\tPaired\tStrand\tall_u+m\tall_u\texon_u+m\texon_u\tfull_u+m\tfull_u"
            ) { meta, row -> 
                "${meta.id}\t${row.join('\t')}"
            }
            .subscribe { __ -> 
                    log.info("Mapping QC stats saved to ${params.outdir}/mapping_qc_stats.tsv")
                }

        ////////////// STEP 4: AGGREGATE METADATA PER SAMPLE ////////////////////////
        // Group all metadata files (public fetched + STARsolo QC) per dataset and
        // aggregate them into a single per-sample CSV and JSON. The aggregator keys
        // on the STARsolo sample name and ignores any file it does not recognise.
        datasetmeta = metadata
            .map { meta, path -> tuple(meta.id, path) }
            .groupTuple()
            .map { dataset_id, paths -> tuple([id: dataset_id], paths.flatten()) }
        
        REPROCESS10X_AGGREGATEMETA(datasetmeta)
        outmetadata = outmetadata.mix(REPROCESS10X_AGGREGATEMETA.out.csv, REPROCESS10X_AGGREGATEMETA.out.json)
        versions = versions.mix(REPROCESS10X_AGGREGATEMETA.out.versions.first())

        REPROCESS10X_AGGREGATEMETA.out.csv
            .splitCsv(sep: ',', skip: 1)
            .collectFile(
                name: 'sample_metadata.csv',
                storeDir: params.outdir,
                newLine: true,
                seed: "${sampleMetaColumns.join(',')}"
            ) { meta, row -> 
                "${meta.id},${row.join(',')}"
            }
            .subscribe { __ -> 
                    log.info("Sample metadata saved to ${params.outdir}/sample_metadata.csv")
                }

        if (!params.collect_metadata) {
            ////////////// STEP 5: LOAD SAMPLES TO iRODS ////////////////////////
            samplecollections = REPROCESS10X_AGGREGATEMETA.out.json
                .splitJson()
                .map { dmeta, smeta -> tuple([id: smeta.sample_id, dataset_id: dmeta.id], smeta) }
                .join(samples)
                .map { key, smeta, path -> tuple(smeta + [dataset_id: key.dataset_id], path, "${params.irodsbase}/${key.dataset_id}/${key.id}") }
                
            irodsfiles = samplecollections
                .flatMap { meta, path, irodspath ->
                    def basedir = path.toString().replaceFirst('/$', '')
                    def samplefiles = files(basedir + '/**', type: 'file')
                    samplefiles.collect { file -> tuple(meta, file, "${irodspath}/${file.toString().replaceFirst(basedir + '/', '')}") }
                }
                .mix(
                    metadata.flatMap { meta, pathlist -> pathlist instanceof List ? pathlist.collect { path -> tuple(meta + [dataset_id: meta.id], path, "${params.irodsbase}/${meta.id}/${path.name}") } : [tuple(meta, pathlist, "${params.irodsbase}/${meta.id}/${pathlist.name}")] }
                )

            // Filter files if ignore_pattern is provided
            if ( ignore_ext ) {
                irodsfiles = irodsfiles.filter { _meta, path, _irodspath ->
                    ignoreExt(path, ignore_ext)
                }
            }
            IRODS_STOREFILE(irodsfiles)
            md5sums = md5sums.mix(IRODS_STOREFILE.out.md5)
            versions = versions.mix(IRODS_STOREFILE.out.versions.first())

            ////////////// STEP 5: ATTACH METADATA ////////////////////////
            outdatasetmeta = metadata.map { meta, _pathlist -> tuple([study_accession_number: meta.id], "${params.irodsbase}/${meta.id}") }.unique()
            outsamplemeta = outsamplemeta.mix(samplecollections.map { meta, _path, irodspath -> tuple(meta, irodspath) })
            
            collectionmeta = outsamplemeta
                .mix(outdatasetmeta)
                .map { meta, irodspath -> tuple(meta + [id: meta.sample_id], irodspath) }
            IRODS_ATTACHCOLLECTIONMETA(collectionmeta)
            versions = versions.mix(IRODS_ATTACHCOLLECTIONMETA.out.versions.first())

            validatecollections = validatecollections.mix(
                IRODS_STOREFILE.out.md5
                    .map { meta, _irodspath, _md5, _imd5 -> meta.dataset_id}
                    .unique()
                    .collect()
                    .flatten()
                    .map { dataset_id -> tuple([id: dataset_id], "${params.irodsbase}/${dataset_id}") }
            )

            outsamplemeta
                .collectFile(
                    name: 'sample_collection_metadata.csv',
                    storeDir: params.outdir,
                    newLine: true,
                    seed: "${sampleMetaColumns.join(',')},irodspath",
                    sort: false,
                ) { meta, irodspath ->
                    def fields = sampleMetaColumns.collect { col ->
                        def value = meta.get(col, '')
                        value instanceof List ? "\"${value.join(',')}\"" : value
                    }
                    "${fields.join(',')},${irodspath}"
                }
                .subscribe { __ ->
                        log.info("Sample collection metadata saved to ${params.outdir}/sample_collection_metadata.csv")
                }
            
            outdatasetmeta
                .collectFile(
                    name: 'dataset_collection_metadata.csv',
                    storeDir: params.outdir,
                    newLine: true,
                    seed: "study_accession_number,irodspath"
                ) { meta, irodspath -> 
                    "${meta.study_accession_number},${irodspath}"
                }
                .subscribe { __ -> 
                        log.info("Dataset collection metadata saved to ${params.outdir}/dataset_collection_metadata.csv")
                    }
        }
    }

    ////////////// STEP 6: VALIDATE UPLOADED COLLECTIONS ////////////////////////
    if (params.validatecollections || (!params.validate_local_only && !params.collect_metadata)) {
        REPROCESS10X_VALIDATEIRODS(toTypedCollection(validatecollections), schema_irods, irodsconfig)
        validateirods = validateirods.mix(fromTypedReport(REPROCESS10X_VALIDATEIRODS.out.txt, validatecollections))

        // out.list carries bare Paths (every task writes "extra_files.list"), and
        // the collection is named inside the file, so no metadata is needed here.
        REPROCESS10X_VALIDATEIRODS.out.list
            .collectFile(
                name: 'irods_extra_files.list',
                storeDir: params.outdir,
                newLine: true,
                sort: true
            ) { path -> path.getText() }
            .subscribe { __ -> 
                    log.info("iRODS extra files list saved to ${params.outdir}/irods_extra_files.list")
                }
    }

    /////////////// COLLECT FILES ////////////////////////
    versions
        .splitText(by: 20)
        .unique()
        .collectFile(name: 'versions.yml', storeDir: params.outdir, sort: true)
        .subscribe { __ -> 
                log.info("Versions saved to ${params.outdir}/versions.yml")
            }
    
    validatelocal
        .collectFile(name: 'localreports.txt', storeDir: params.outdir) {_meta, path -> path.getText() }
        .subscribe { __ ->
                log.info("Local validation reports saved to ${params.outdir}/localreports.txt")
            }

    validateirods
        .collectFile(name: 'irodsreports.txt', storeDir: params.outdir) {_meta, path -> path.getText() }
        .subscribe { __ ->
                log.warn("iRODS validation reports saved to ${params.outdir}/irodsreports.txt. Please check the reports for any errors or warnings.")
            }

    md5sums
        .collectFile(name: 'md5sums.tsv', storeDir: params.outdir) {_meta, irodspath, md5, irods_md5 -> "${irodspath}\t${md5}\t${irods_md5}\n" }
        .subscribe { __ ->
                log.info("MD5 sums saved to ${params.outdir}/md5sums.tsv")
            }
    
    publish:
    localreports = validatelocal.map { meta, path -> meta + [path: path] }
    irodsreports = validateirods.map { meta, path -> meta + [path: path] }
    metadata     = metadata.mix(outmetadata).map { meta, path -> meta + [path: path] }
}

output {
    localreports {
        label "report"
        index {
            path "index/localreports.csv"
            header true
            sep ','
        }
        path "localreports"
    }
    irodsreports {
        label "report"
        index {
            path "index/irodsreports.csv"
            header true
            sep ','
        }
        path "irodsreports"
    }
    metadata {
        label "metadata"
        index {
            path "index/metadata.csv"
            header true
            sep ','
        }
        path { meta -> "metadata/${meta.id}" }
    }
}