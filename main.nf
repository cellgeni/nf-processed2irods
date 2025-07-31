include { REPROCESS10X_PARSEMETADATA } from './modules/local/reprocess10x/parsemetadata'
include { REPROCESS10X_MAPPINGQC } from './modules/local/reprocess10x/mappingqc'


workflow {
    // Read datasets from a CSV file
    datasets = channel.fromPath(params.datasets, checkIfExists: true)
                      .splitCsv(header: true)
                      .map { row -> tuple(row, row.path)}
    
    //datasets.view()

    // Get metadata for all samples
    samples = datasets.map { row -> 
                    [
                        row[0],
                        file(row[1] + '/[A-Z]*[0-9]*', type: 'dir').collect { it.baseName }
                    ]
                }
    REPROCESS10X_PARSEMETADATA(samples)
    metadata = REPROCESS10X_PARSEMETADATA.out.tsv
    //metadata.view()

    // Get solo QC stats
    REPROCESS10X_MAPPINGQC(datasets)
    mappingqc = REPROCESS10X_MAPPINGQC.out.tsv
    //mappingqc.view()

    // Get a list of all files to move
    mapping_results = datasets.map { row -> 
                    [
                        row[0],
                        file(row[1] + '/[A-Z]*[0-9]*/**', type: 'file')
                    ]
                }
                .transpose()
    

}