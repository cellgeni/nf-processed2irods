include { REPROCESS10X_VALIDATELOCAL } from './modules/local/reprocess10x/validatelocal'
include { REPROCESS10X_IRODSBARCODESANDLOGS } from './modules/local/reprocess10x/irodsbarcodesandlogs'
include { REPROCESS10X_AGGREGATEMETA } from './modules/local/reprocess10x/aggregatemeta'
include { REPROCESS10X_VALIDATEIRODS } from './modules/local/reprocess10x/validateirods'
include { IRODS_ATTACHCOLLECTIONMETA } from './modules/local/irods/attachcollectionmeta'
include { IRODS_STOREFILE } from './modules/local/irods/storefile'
include { FETCH10XMETA } from 'cellgeni/fetch10xmeta'
include { STARSOLOQC } from 'cellgeni/starsoloqc'

def checkIfPublic(series) {
    return (series ==~ /GSE\d+/) || (series ==~ /E-MTAB-\d+/) || (series ==~ /PRJ.{0,3}\d+/)
}

def ignoreExt(path, ignore_ext) {
    return !ignore_ext.any { ext -> path.name.contains(ext) }
}

workflow {
    main:
    /////////////// PARAMETER INITIALIZATION ////////////////////////
    def ignore_ext = params.ignore_pattern ? params.ignore_pattern.split(',').collect { ext -> ext.trim() }.findAll { ext -> ext } : []
    def sampleMetaColumns = [
                'sample_id', 'dataset_id', 'species', 'paired', 'strand', 'total_reads',
                'whitelist', 'cells', 'geo_sample', 'sample', 'experiment', 'run',
            ]

    /////////////// INITIALIZE CHANNELS ////////////////////////
    versions            = channel.empty()
    metadata            = channel.empty()
    outmetadata         = channel.empty()
    outdatasetmeta      = channel.empty()
    outsamplemeta       = channel.empty()
    validatelocal       = channel.empty()
    validateirods       = channel.empty()
    md5sums             = channel.empty()
    samples             = params.samples && !params.validatecollections ? channel.fromPath(params.samples, checkIfExists: true) : channel.empty()
    irodsconfig         = params.irodsconfig ? channel.value(file(params.irodsconfig, type: 'file', checkIfExists: true)) : channel.empty()
    validatecollections = params.validatecollections ? channel.fromPath(params.validatecollections, checkIfExists: true) : channel.empty()
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
    REPROCESS10X_VALIDATELOCAL(datasets)
    validatelocal = validatelocal.mix(REPROCESS10X_VALIDATELOCAL.out.txt)
    versions = versions.mix(REPROCESS10X_VALIDATELOCAL.out.versions.first())

    if (!params.validate_local_only) {
        /////////////// STEP 1.2: CHECK THAT SAMPLES ARE NOT ON IRODS ///////////////////////
        samples
            .filter { meta, _path -> 
                def collection = "${params.irodsbase}/${meta.dataset_id}/${meta.id}".toString()
                def exists = ["ils", collection].execute()
                exists.waitFor()
                exists.exitValue() == 0
            }
            .view { meta, _path ->
                log.warn("Sample ${meta.id} from dataset ${meta.dataset_id} already exists on iRODS. Exiting...")
            }
            .collect()
            .subscribe { irodscollected -> 
                if (irodscollected.size() > 0) {
                    error("One or more samples already exist on iRODS. Exiting...")
                }
            }

        /////////////// STEP 2: FETCH METADATA ////////////////////////
        public_datasets = REPROCESS10X_VALIDATELOCAL.out.dataset
            .filter { meta, _pathlist -> checkIfPublic(meta.id) }
            .map { meta, pathlist ->
                def paths = pathlist instanceof List ? pathlist : [pathlist] // ensure pathlist is a list
                def processedsamples = paths.findAll { it -> it.isDirectory() }.collect { it -> it.baseName }
                def proc = ["ils", "${params.irodsbase}/${meta.id}".toString()].execute() // check if the dataset already exists on iRODS
                proc.waitFor()
                def uploadedsamples = proc.exitValue() == 0 ?
                    proc.in.text.readLines().findAll { it -> it.trim().startsWith('C- ') }.collect { it -> it.trim().split('/')[-1] } :
                    []
                tuple(meta, (processedsamples + uploadedsamples).unique().sort().join(','))
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
        processedsamples = REPROCESS10X_VALIDATELOCAL.out.dataset
            .flatMap { meta, _pathlist ->
                def proc = ["ils", "${params.irodsbase}/${meta.id}".toString()].execute() // check if the dataset already exists on iRODS
                proc.waitFor()
                def uploadedsamples = proc.exitValue() == 0 ?
                    proc.in.text.readLines().findAll { it -> it.trim().startsWith('C- ') }.collect { it -> it.trim().substring(3).trim() } :
                    []
                uploadedsamples.collect { irodspath -> [[id: irodspath.split('/')[-1], dataset_id: meta.id], irodspath] }
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
        REPROCESS10X_VALIDATEIRODS(validatecollections, irodsconfig)
        validateirods = validateirods.mix(REPROCESS10X_VALIDATEIRODS.out.txt)
        versions = versions.mix(REPROCESS10X_VALIDATEIRODS.out.versions.first())
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