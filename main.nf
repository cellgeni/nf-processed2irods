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
    datasets = datasets
        .splitCsv(header: true, sep: ',')
        .map { row -> tuple([id: row.id], files("${row.path}/*", type: 'any', checkIfExists: true).sort())}

    samples = samples
        .splitCsv(header: true, sep: ',')
        .map { row -> tuple([id: row.dataset_id], file(row.path, type: 'dir', checkIfExists: true))}
        .groupTuple(sort: 'hash')

    /////////////// STEP 1: VALIDATE LOCAL ///////////////
    REPROCESS10X_VALIDATELOCAL(datasets.mix(samples))

    /////////////// STEP 2: FETCH METADATA ////////////////////////
    if (params.fetchpublic) {
        public_datasets = REPROCESS10X_VALIDATELOCAL.out.dataset
            .filter { meta, _pathlist -> checkIfPublic(meta.id) }
            .map { meta, pathlist ->
                tuple(meta, pathlist.findAll { it -> it.isDirectory() }.collect { it -> it.baseName }.sort().join(','))
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