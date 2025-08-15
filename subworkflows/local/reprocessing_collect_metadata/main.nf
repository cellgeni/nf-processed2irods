include { REPROCESS10X_PARSEMETADATA } from '../../../modules/local/reprocess10x/parsemetadata'
include { REPROCESS10X_MAPPINGQC } from '../../../modules/local/reprocess10x/mappingqc'


def checkIfPublic(series) {
    return (series =~ /GSE\d+/) || (series =~ /E-MTAB-\d+/) || (series =~ /PRJEB\d+/)
}

// Workflow to collect metadata for processed samples
workflow REPROCESSING_COLLECT_METADATA {

    take:
    samples                       // channel: [ [id: dataset_id], [ path(sample_dir1), path(sample_dir2), ...] ]
    parse_mapper_metrics_flag     // boolean: true/false
    collect_public_metadata_flag  // boolean: true/false
    verbose                       // boolean: true/false
    main:
    // STEP 1: Parse mapping metadata if parse_mapper_metrics_flag is true
    // Look for existing QC stats in sample directories
    parse_mapper_metrics = parse_mapper_metrics_flag ? samples : channel.empty()
    parse_mapper_metrics = parse_mapper_metrics.branch { meta, pathlist ->
            def qc_stats = meta.path ? file("${meta.path}/*solo_qc.tsv").find() : null
            qc: !qc_stats
            skip: true
                return [
                    meta,
                    qc_stats
                ]
        }
    
    // Report to user if verbose is enabled
    if ( verbose ) {
        parse_mapper_metrics.skip.view { meta, qc_path ->
            "STEP1: Found QC stats for ${meta.id} at ${qc_path.name}"
        }
        parse_mapper_metrics.qc.view { meta, pathlist ->
            def sample_string = pathlist.size() > 1 ?  "with samples: ${pathlist.collect { it.baseName }.join(', ')}" : ''
            "STEP1: Parsing STARsolo QC metrics for ${meta.id} ${sample_string}"
        }
    }
    // Parse QC stats for samples with no QC files
    REPROCESS10X_MAPPINGQC(parse_mapper_metrics.qc)
    mappingqc = REPROCESS10X_MAPPINGQC.out.tsv.mix(parse_mapper_metrics.skip)

    // STEP 2: Collect public metadata if collect_public_metadata_flag is true
    // Find Public datasets
    collect_public_metadata = collect_public_metadata_flag ? samples : channel.empty()
    collect_public_metadata = collect_public_metadata.filter { meta, pathlist ->
            checkIfPublic(meta.id)
        }
        .map { meta, pathlist ->
            tuple(meta, pathlist.collect { it.baseName })
        }
    
    // Report to user if verbose is enabled
    if ( verbose ) {
        collect_public_metadata.view { meta, sample_ids ->
            "STEP2: Collecting public metadata for ${meta.id} with samples: ${sample_ids.join(', ')}"
        }
    }

    // Parse public metadata
    REPROCESS10X_PARSEMETADATA(collect_public_metadata)
    public_metadata = REPROCESS10X_PARSEMETADATA.out.tsv
        .transpose()
        .mix(
            REPROCESS10X_PARSEMETADATA.out.soft,
            REPROCESS10X_PARSEMETADATA.out.txt.transpose(),
            REPROCESS10X_PARSEMETADATA.out.list.transpose(),
            )

    // Collect versions of the tools used
    versions = REPROCESS10X_MAPPINGQC.out.versions.mix(
        REPROCESS10X_PARSEMETADATA.out.versions
    )
    emit:
    mapping_metadata = mappingqc
    public_metadata = public_metadata
    versions = versions
    
}
