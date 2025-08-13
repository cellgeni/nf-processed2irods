include { REPROCESS10X_PARSEMETADATA } from '../../../modules/local/reprocess10x/parsemetadata'
include { REPROCESS10X_MAPPINGQC } from '../../../modules/local/reprocess10x/mappingqc'


def checkIfPublic(series) {
    return (series =~ /GSE\d+/) || (series =~ /E-MTAB-\d+/) || (series =~ /PRJEB\d+/)
}

// Workflow to collect metadata for processed samples
workflow REPROCESSING_COLLECT_METADATA {

    take:
    samples                       // channel: [ [id: dataset_id], [ path(sample_dir1), path(sample_dir2), ...] ]
    collect_public_metadata_flag  // boolean: true/false
    parse_mapper_metrics_flag     // boolean: true/false
    
    main:
    // samples.tap { parse_mapper_metrics }
    //        .set { collect_public_metadata }
    // Parse mapping metadata if required
    parse_mapper_metrics = parse_mapper_metrics_flag ? samples : channel.empty()
    // parse_mapper_metrics.view { meta, pathlist ->
    //     "Parsing mapper metrics for ${meta.id} with samples: ${pathlist.collect { it.baseName }.join(', ')}"
    // }
    REPROCESS10X_MAPPINGQC(parse_mapper_metrics)

    // Collect public metadata if required
    collect_public_metadata = samples.filter { meta, pathlist ->
            checkIfPublic(meta.id)
        }
        .map { meta, pathlist ->
            tuple(meta, pathlist.collect { it.baseName })
        }
    collect_public_metadata = collect_public_metadata_flag ? collect_public_metadata : channel.empty()
    // collect_public_metadata.view { meta, sample_ids ->
    //     "Collecting public metadata for ${meta.id} with samples: ${sample_ids.join(', ')}"
    // }
    REPROCESS10X_PARSEMETADATA(collect_public_metadata)
}
