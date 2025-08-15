include { DATASETS2IRODS } from './subworkflows/local/datasets2irods'
include { REPROCESSING_COLLECT_METADATA } from './subworkflows/local/reprocessing_collect_metadata'
include { IRODS_UPLOAD_COLLECTION } from './subworkflows/local/irods_upload_collection'

def helpMessage() {
    log.info(
      """
      ===================
      nf-processed2irods pipeline
      ===================
      This Nextflow pipeline stores processed single-cell genomics data and comprehensive metadata in iRODS for long-term data management and sharing.

      Usage: nextflow run main.nf [parameters]

      Required parameters:
        --datasets <string>       Path to a CSV file containing dataset information with columns: 'id' (dataset identifier) and 'path' (local filesystem path to processed data directory)
        --irodspath <string>      Base path in iRODS where datasets will be stored (e.g., "/archive/cellgeni/sanger/")

      Optional parameters:
        --help                    Display this help message
        --remove_unmapped_reads   Remove unmapped read files (*.Unmapped.out.mate*.bz2) to save storage space (default: true)
        --output_dir              Output directory for pipeline results (default: "results")
        --publish_mode            File publishing mode (default: "copy")

      Input file format:
        The --datasets parameter expects a CSV file with the following structure:
        
        id,path
        GSE123456,/path/to/processed/GSE123456
        PRJEB12345,/path/to/processed/PRJEB12345
        EGA_DATASET,/path/to/processed/EGA_DATASET

      Pipeline workflow:
        1. Dataset Discovery - Reads dataset information from CSV input file
        2. Public Dataset Detection - Identifies public datasets (GSE*, E-MTAB-*, PRJEB* patterns)
        3. Metadata Parsing - Extracts metadata from public repositories for public datasets
        4. Quality Control - Generates mapping QC statistics from STARsolo output (if not present)
        5. File Collection - Gathers all data files and metadata files for upload
        6. iRODS Upload - Transfers files to iRODS with checksums
        7. Metadata Attachment - Attaches comprehensive metadata to iRODS collections

      Examples:
        # Basic usage - Upload processed datasets to iRODS
        nextflow run main.nf --datasets datasets.csv --irodspath "/archive/cellgeni/sanger/"
        
        # Remove unmapped reads to save storage space (default behavior)
        nextflow run main.nf --datasets datasets.csv --irodspath "/archive/cellgeni/sanger/" --remove_unmapped_reads true
        
        # Keep all files including unmapped reads
        nextflow run main.nf --datasets datasets.csv --irodspath "/archive/cellgeni/sanger/" --remove_unmapped_reads false
        
        # Custom output directory
        nextflow run main.nf --datasets datasets.csv --irodspath "/archive/cellgeni/sanger/" --output_dir "my_results"

      Expected data structure:
        Each dataset directory should contain:
        - Sample directories: Named with sample identifiers containing STARsolo output files
        - QC files: *solo_qc.tsv files (generated automatically if missing)
        - Metadata files: For public datasets, metadata will be automatically retrieved

      Metadata handling:
        - Public datasets (GSE*, E-MTAB-*, PRJEB*): Automatic metadata retrieval from public repositories
        - All datasets: Extraction of sample-level metadata (species, sequencing type, strand, read counts, whitelist version)
        - iRODS collections: Comprehensive metadata attachment for searchability and provenance

      For more details, see the README.md file in this repository.
      """.stripIndent()
    )
}

def missingParametersError() {
    log.error("Missing input parameters")
    helpMessage()
    error("Please provide all required parameters: --datasets and --irodspath. See --help for more information.")
}

def findSampleDirectories(path) {
    // Find all directories in the given path that are not raw or fastq directories
    def exclusions = ['raw', 'fastq', 'temp', 'log', 'done_wget']
    return files(path + '/*', type: 'dir').findAll { dir ->
        !exclusions.any { pattern -> dir.name.toLowerCase().contains(pattern) }
    }
}


workflow {
    if (params.help) {
        helpMessage()
    } else if (!(params.datasets || params.samples) || !params.irodspath) {
        missingParametersError()
    }
    
    // STEP 0: Read input files
    // Read datasets list
    channel.fromPath(params.datasets)
        // Read CSV files to channel
        .splitCsv(header: true, sep: ',')
        // Add dataset path to each dataset metadata
        .map { meta ->
          tuple( meta, meta.path )
        }
        // Save dataset list to a channel
        .tap { datasets }
        // Get all samples in dataset directory
        .map { meta, path -> 
            def unified_path = path.toString().replaceFirst('/$', '')
            def sample_list = findSampleDirectories(unified_path)
            [
                meta,
                sample_list
            ]
        }
        // Save to a channel
        .tap { grouped_dataset_samples }
        // Flatten the channel from [meta, [dir1, dir2, ...]] -> [meta, dir1], [meta, dir2], ...
        .transpose()
        // Update metadata for each sample
        .map { meta, path -> 
            [
                [id: path.name, dataset_id: meta.id],
                path
            ]
        }
        .set { dataset_samples }


    // Read samples list if provided
    channel.fromPath(params.samples)
        // Read CSV files to channel
        .splitCsv(header: true, sep: ',')
        // Add sample path to each sample metadata
        .map { meta ->
            tuple( meta, file(meta.path, type: 'dir') )
        }
        // Save to a channel
        .tap { samples }
        // Group samples by dataset_id (if specified)
        .map { meta, path ->
            def dataset_id = meta.dataset_id ?: "${meta.id}"
            [
                [id: dataset_id], // grouping metadata
                ["${path.name}": meta.id], // sample_id ~ dirname relation
                path
            ]
        }
        .groupTuple(sort: 'hash')
        // Add sample_id ~ dirname relation as mapper QC metadata script constructs file based on sample directories
        .map{ meta, id_file_relation, pathlist ->
            def combined_relation_list = id_file_relation.inject([:]) { result, dict -> result + dict }
            tuple( meta + [relation: combined_relation_list], pathlist ) 
        }
        .set { grouped_samples }

    // STEP 1: Collect metadata
    // Run metadata collection
    grouped_samples = grouped_samples.mix(grouped_dataset_samples)
    grouped_samples.view { meta, pathlist -> 
        "Dataset ID: ${meta.id}, Sample IDs: ${pathlist.collect { it.baseName }.join(', ')}"
    }
    REPROCESSING_COLLECT_METADATA(
        grouped_samples,
        params.parse_mapper_metrics,
        params.collect_public_metadata,
        params.verbose
    )

    // Read metadata to channel
    mapping_metadata = REPROCESSING_COLLECT_METADATA.out
        .mapping_metadata
        .splitCsv(header: true, sep: '\t')
        .map { meta, meta_qc -> 
            def sample_id = meta.relation ? meta.relation.get("${meta_qc.Sample}", meta_qc.Sample) : meta_qc.Sample
            def dataset_id = meta.relation && meta.relation.size() == 1 ? "-" : meta.id
            tuple([id: sample_id, dataset_id: dataset_id], meta_qc)
        }
    
    relation_metadata = REPROCESSING_COLLECT_METADATA.out
        .public_metadata
        .filter{ meta, file -> file.name =~ /.*accessions.tsv/ }
        .splitCsv(header: ["geo_sample", "sample", "experiment", "run"], sep: '\t')
        .map { meta, meta_rel ->
            [
                [
                    id: meta_rel.geo_sample != "-" ? meta_rel.geo_sample : meta_rel.sample,
                    dataset_id: meta.id
                ],
                meta_rel
            ]
        }
    
    metadata = mapping_metadata
        .mix(relation_metadata)
        .groupTuple(sort: 'hash')
        .map { meta, meta_list ->
            // Combine all metadata dictionaries in the list
            def combined_meta = meta_list.inject([:]) { result, dict -> result + dict }
            [
                [id: meta.id, dataset_id: meta.dataset_id],
                [
                    id: meta.id,
                    series: meta.dataset_id,
                    paired: combined_meta.Paired,
                    species: combined_meta.Species,
                    strand: combined_meta.Strand,
                    total_reads: combined_meta.Rd_all,
                    whitelist: combined_meta.WL,
                    geo_sample: combined_meta.geo_sample ?: "-",
                    sample: combined_meta.sample ?: meta.id,
                    experiment: combined_meta.experiment ?: "-",
                    run: combined_meta.run ?: "-"
                ]
            ]
        }

    // // STEP 2: Upload sample directories to iRODS
    samples = samples
        // Attach iRODS path to each sample
        .mix(dataset_samples)
        .map { meta, path ->
            def unified_path = params.irodspath.replaceFirst('/$', '')
            def collection_name = meta.dataset_id ? "${meta.dataset_id}/${meta.id}" : meta.id
            def irodspath = "${unified_path}/${collection_name}"
            tuple([id: meta.id, dataset_id: meta.dataset_id ?: "-"], path, irodspath)
        }
        // Attach metadata to samples channel
        .join(metadata)
        .map{ group_meta, path, irodspath, meta -> tuple(meta, path, irodspath) }
    
    // Save metadata to file for the user
    samples.collectFile(name: 'metadata.csv', newLine: false, storeDir: params.output_dir, sort: true, keepHeader: true, skip: 1) { sample_meta, path, irodspath -> 
        def meta_values = sample_meta.values().join(',')
        def meta_keys = sample_meta.values().join(',')
        def header = "sample_id,path,irodspath," + sample_meta.keySet().join(',')
        def line = "${sample_meta.id},${path},${irodspath},${meta_values}"
        "${header}\n${line}\n"
        
    }


    IRODS_UPLOAD_COLLECTION(
        samples,
        params.ignore_pattern,
        params.verbose
    )
}   