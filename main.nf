include { REPROCESS10X_VALIDATELOCAL } from './modules/local/reprocess10x/validatelocal'
include { FETCH10XMETA } from 'cellgeni/fetch10xmeta'
include { STARSOLOQC } from 'cellgeni/starsoloqc'

def checkIfPublic(series) {
    return (series =~ /GSE\d+/) || (series =~ /E-MTAB-\d+/) || (series =~ /PRJ.{0,3}\d+/)
}

workflow {
    main:
    /////////////// INITIALIZE CHANNELS ////////////////////////
    metadata = channel.empty()
    datasets = params.datasets ? channel.fromPath(params.datasets, checkIfExists: true) : channel.empty()
    samples = params.samples ? channel.fromPath(params.samples, checkIfExists: true) : channel.empty()

    /////////////// STEP 0: INPUTS ///////////////////////
    samples = samples
        .splitCsv(header: true, sep: ',')
        .map { row -> tuple([id: row.id, dataset_id: row.dataset_id], file(row.path, type: 'dir', checkIfExists: true))}
    
    datasets = samples
        .map { meta, path -> tuple(meta.dataset_id, meta.id, path) }
        .groupTuple(sort: 'hash')
        .map { dataset_id, samplelist, paths -> tuple([id: dataset_id, samples: samplelist], paths) }

    /////////////// STEP 1.1: CHECK THAT SAMPLES ARE NOT ON IRODS ///////////////////////
    irodssamples = samples
        .filter { meta, _path -> 
            def collection = params.irodsbase + "/${meta.dataset_id}/${meta.id}"
            def exists = "ils ${collection}".execute()
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

    /////////////// STEP 1.2: VALIDATE LOCAL DIRECTORIES ///////////////
    REPROCESS10X_VALIDATELOCAL(datasets)

    /////////////// STEP 2: FETCH METADATA ////////////////////////
    if (params.fetchpublic) {
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
        metadata = metadata.mix(
            FETCH10XMETA.out.tsv,
            FETCH10XMETA.out.list,
            FETCH10XMETA.out.soft,
            FETCH10XMETA.out.txt
        )
    }

    ////////////// STEP 3: COLLECT STARSOLO QC ////////////////////////
    if (params.soloqc) {
        noqcdatasets = REPROCESS10X_VALIDATELOCAL.out.dataset
            .filter { _meta, pathlist ->  !pathlist.any { it.name ==~ /.*solo_qc.*\.tsv/ }}
        STARSOLOQC(noqcdatasets)
        metadata = metadata.mix(STARSOLOQC.out.tsv)
    }

    /////////////// COLLECT FILES ////////////////////////
    REPROCESS10X_VALIDATELOCAL.out.versions.first()
        .splitText(by: 20)
        .unique()
        .collectFile(name: 'versions.yml', storeDir: params.outdir, sort: true)
        .subscribe { __ -> 
                log.info("Versions saved to ${params.outdir}/versions.yml")
            }
    
    REPROCESS10X_VALIDATELOCAL.out.txt
        .collectFile(name: 'localreports.txt', storeDir: params.outdir) {_meta, path -> path.getText() }
        .subscribe { __ -> 
                log.info("Local validation reports saved to ${params.outdir}/localreports.txt")
            }
    
    publish:
    localreports = REPROCESS10X_VALIDATELOCAL.out.txt.map { meta, path -> meta + [path: path] }
    metadata     = metadata.map { meta, path -> meta + [path: path] }
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