include { REPROCESS10X_PARSEMETADATA } from './modules/local/reprocess10x/parsemetadata'
include { REPROCESS10X_MAPPINGQC } from './modules/local/reprocess10x/mappingqc'
include { IRODS_STOREFILE } from './modules/local/irods/storefile'


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

    // Get a list of all files mapping results
    mapping = datasets.map { row -> 
                    [
                        row[0],
                        // Here I look for STARsolo output directorues
                        file(row[1] + '/[A-Z]*[0-9]*/**', type: 'file')
                    ]
                }
                // flatten file lists [meta, [file1, file2, ...]] -> [meta, file1], [meta, file2], ...
                .transpose()
                // Add target iRODS path to the list
                .map {
                    meta, path ->
                    def relativepath = path.toString().replaceFirst(meta.path, '').replaceFirst('/', '')
                    def irodspath = params.irodspath.replaceFirst('/$', '') + "/${meta.id}/" + relativepath
                    tuple([id: meta.id, local_dataset_path: meta.path], path, irodspath)
                }
    // Get a list of all metadata files and QC stats
    metadata_qc = REPROCESS10X_PARSEMETADATA.out.tsv
                                                .mix(
                                                    REPROCESS10X_PARSEMETADATA.out.list,
                                                    REPROCESS10X_PARSEMETADATA.out.links,
                                                    REPROCESS10X_PARSEMETADATA.out.txt,
                                                    REPROCESS10X_PARSEMETADATA.out.soft
                                                )
                                                // flatten file lists [meta, [file1, file2, ...]] -> [meta, file1], [meta, file2], ...
                                                .transpose()
                                                // attach mapping QC stats
                                                .mix(
                                                    mappingqc
                                                )
                                                // Add target iRODS path to the list
                                                .map {
                                                    meta, path ->
                                                    def irodspath = params.irodspath.replaceFirst('/$', '') + "/${meta.id}/" + path.name
                                                    tuple([id: meta.id, local_dataset_path: meta.path], path, irodspath)
                                                }
    // Combine mapping and metadata files
    filesToLoad = mapping.mix(metadata_qc).filter { meta, path, irodspath ->
        irodspath.toString() =~ /.*solo_qc.*/
    }
    filesToLoad.toSortedList().view()
    // Upload files to iRODS
    IRODS_STOREFILE(filesToLoad)

    IRODS_STOREFILE.out.md5.view()

    // Attach metadata
    
}