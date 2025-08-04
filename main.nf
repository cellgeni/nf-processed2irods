include { REPROCESS10X_PARSEMETADATA } from './modules/local/reprocess10x/parsemetadata'
include { REPROCESS10X_MAPPINGQC } from './modules/local/reprocess10x/mappingqc'
include { IRODS_STOREFILE } from './modules/local/irods/storefile'
include { IRODS_ATTACHCOLLECTIONMETA } from './modules/local/irods/attachcollectionmeta'

def checkIfPublic(series) {
    return (series =~ /GSE\d+/) || (series =~ /E-MTAB-\d+/) || (series =~ /PRJEB\d+/)
}

workflow {
    // Read datasets from a CSV file
    channel.fromPath(params.datasets, checkIfExists: true)
           .splitCsv(header: true)
           .map { row -> tuple(row, row.path)}
           // Save all datasets to a variable
           .tap { datasets }
           // Save public datasets to a variable
           .filter { meta, _path -> checkIfPublic(meta.id) }
           .set { public_datasets }
    
    // public_datasets.view {meta, path -> "Public dataset: ${meta.id} at ${path}"}

    // Get public metadata
    public_datasets
                // get all sample names
                .map { meta, path -> 
                    [
                        meta,
                        file(path + '/[A-Z]*[0-9]*', type: 'dir').collect { it.baseName }
                    ]
                }
                // save to variable
                .tap { public_datasets_samples }
                // notify user if there are no public datasets
                .count()
                .subscribe { count ->
                    if (count == 0) {
                        log.info "No public datasets found. Skipping REPROCESS10X_PARSEMETADATA step..."
                    }
                }
    REPROCESS10X_PARSEMETADATA(public_datasets_samples)
    public_metadata = REPROCESS10X_PARSEMETADATA.out.tsv
    // public_metadata.view { meta, files -> 
    //     "Public metadata files for ${meta.id}: ${files.collect { it.name }.join(', ')}"
    // }

    // Check if qc stats exist for the datasets
    // datasets.view { meta, path -> "Dataset ${meta.id} at ${path}" }
    datasets_for_qc = datasets
                              .branch { meta, path ->
                                  qc: file(path + "/*solo_qc.tsv", type: 'file').isEmpty()
                                  skip: true
                                    return [
                                        meta,
                                        file(path + "/*solo_qc.tsv", type: 'file').find()
                                    ]
                              }

    // notify user if no datasets need mapping QC stats parsing
    datasets_for_qc.qc.count().subscribe { count ->
        if (count == 0) {
            log.info "No datasets need mapping QC stats calculation. Skipping REPROCESS10X_MAPPINGQC step..."
        }
    }

    // datasets_for_qc.qc.view {meta, path -> "Dataset ${meta.id} needs QC stats at ${path}"}
    // datasets_for_qc.skip.view {meta, path -> "Dataset ${meta.id} already has QC stats at ${path}"}

    // Run mapping QC stats collection for datasets that don't have them
    REPROCESS10X_MAPPINGQC(datasets_for_qc.qc)
    mappingqc = REPROCESS10X_MAPPINGQC.out.tsv.mix(datasets_for_qc.skip)
    // mappingqc.view {meta, qc_file -> 
    //     "Mapping QC stats for ${meta.id}: ${qc_file.name}"
    // }

    // // Get a list of all files mapping results
    mapping_files = datasets
                            // get a list of all directories
                            .map { meta, path -> 
                                [
                                    meta,
                                    // Here I look for STARsolo output directorues
                                    file(path + '/*', type: 'dir')
                                ]
                            }
                            // flatten file lists [meta, [dir1, dir2, ...]] -> [meta, dir1], [meta, dir2], ...
                            .transpose()
                            // Remove unwanted directories
                            .filter { meta, dir ->
                                !(dir.name =~ /done_wget/) &&
                                !(dir.name =~ /fastq/)
                            }
                            // get a list of all files in sample directories
                            .map { meta, dir ->
                                [
                                    meta,
                                    // Here I look for STARsolo output files
                                    file(dir + '/**', type: 'file')
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

    // // Get a list of all metadata files and QC stats
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
                                                // Remove unwanted files
                                                .filter { meta, path, irodspath -> 
                                                    !(irodspath =~ /.*wget-log.*/) && 
                                                    !(irodspath =~ /.*fastq.*/) && 
                                                    !(irodspath =~ /.*done_wget.*/) && 
                                                    !(irodspath =~ /.*\.sh/) && 
                                                    !(irodspath =~ /.*\.bsub.*/) && 
                                                    !(irodspath =~ /.*\.pl/)
                                                }

    // Remove unmapped reads if specified
    if (params.remove_unmapped_reads) {
        mapping_files = mapping_files.filter { meta, path, irodspath -> !(path.name =~ /Unmapped.out.mate[12].bz2/) }
    }
    // Combine mapping and metadata files
    filesToLoad = mapping_files.mix(metadata_qc)
    // filesToLoad.collectFile(name: "loadfiles.tsv", storeDir: "/lustre/scratch127/cellgen/cellgeni/aljes/reprocessing") { meta, path, irodspath ->
    //         "${meta.id}\t${path}\t${irodspath}\n"
    // }
    
    // Upload files to iRODS
    IRODS_STOREFILE(filesToLoad)

    // Attach metadata
    mapping_metadata = mappingqc
                            .splitCsv(header: true, sep: '\t')
                            .map { meta, qc_meta -> 
                                    [
                                        [id: qc_meta.Sample, dataset_id: meta.id],
                                        [
                                            'series': meta.id,
                                            'paired': qc_meta.Paired,
                                            'species': qc_meta.Species,
                                            'strand': qc_meta.Strand,
                                            'total_reads': qc_meta.Rd_all,
                                            'whitelist': qc_meta.WL
                                        ]
                                    ]
                            }

    relation_metadata = public_metadata
                                .transpose()
                                .filter{ meta, file -> file.name =~ /.*accessions.tsv/ }
                                .splitCsv(header: ["geo_sample", "sample", "experiment", "run"], sep: '\t')
                                .map { meta, rel_meta ->
                                    [
                                        [
                                            id: rel_meta.geo_sample != "-" ? rel_meta.geo_sample : rel_meta.sample,
                                            dataset_id: meta.id
                                        ],
                                        rel_meta
                                    ]
                                }
    
    // sample_collections = IRODS_STOREFILE.out.md5
    //                             .filter { meta, irodspath, md5 -> irodspath =~ /.*Log.final.out/ }
    //                             .map { meta, irodspath, md5 ->
    //                                 [
    //                                     [id: irodspath.parent.name, dataset_id: meta.id],
    //                                     irodspath.parent
    //                                 ]
    //                             }
    //                             .view()

    sample_metadata = mapping_metadata
                                      .mix(relation_metadata)
                                      .groupTuple()
                                      .map { meta, meta_list -> 
                                        // Combine all metadata dictionaries in the list
                                        def combined_meta = meta_list.inject([:]) { result, dict -> result + dict }
                                        // Add 'sample' key if it doesn't exist
                                        combined_meta.sample = combined_meta.get('sample') ?: meta.id
                                        [
                                            [id: meta.id] + combined_meta,
                                            params.irodspath.replaceFirst('/$', '') + "/${meta.dataset_id}/${meta.id}"
                                        ]
                                      }
    dataset_metadata = datasets.map { meta, path ->
                        def irodspath = params.irodspath.replaceFirst('/$', '') + "/${meta.id}"
                        tuple( [id: meta.id, study_accession_number: meta.id], irodspath)
                        }
    metadata = sample_metadata.mix(dataset_metadata)
    // metadata.view { meta, irodspath -> 
    //     "Metadata for ${meta.id} at ${irodspath}"
    // }
    IRODS_ATTACHCOLLECTIONMETA(metadata)
}