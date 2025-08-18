include { REPROCESSING_COLLECT_METADATA } from './subworkflows/local/reprocessing_collect_metadata'
include { IRODS_UPLOAD_COLLECTION } from './subworkflows/local/irods_upload_collection'
include { IRODS_ATTACHCOLLECTIONMETA } from './modules/local/irods/attachcollectionmeta'
include { IRODS_STOREFILE } from './modules/local/irods/storefile'
include { checkIfPublic } from './subworkflows/local/reprocessing_collect_metadata'

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
        OR
        --samples <string>        Path to a CSV file containing sample information with columns: 'id' (sample identifier), 'path' (local filesystem path), and optionally 'dataset_id'
        --irodspath <string>      Base path in iRODS where datasets will be stored (e.g., "/archive/cellgeni/sanger/")

      Optional parameters:
        --help                    Display this help message
        --output_dir              Output directory for pipeline results (default: "results")
        --publish_mode            File publishing mode (default: "copy")
        --ignore_pattern          Comma-separated list of file patterns to ignore during upload (default: ".bam,.bai,.cram,.crai,.fastq.gz,.fq.gz,.fastq,.fq,.mate1.bz2,.mate2.bz2,.sh,.bsub,.pl")
        --collect_public_metadata Collect metadata from public repositories for public datasets (default: true)
        --parse_mapper_metrics    Parse mapping QC metrics from STARsolo output (default: true)
        --verbose                 Enable verbose output (default: false)
        --dry_run                 Perform a dry run without uploading files to iRODS (default: false)

      Input file formats:
        
        For --datasets parameter (CSV file):
        id,path
        GSE123456,/path/to/processed/GSE123456
        PRJEB12345,/path/to/processed/PRJEB12345
        EGA_DATASET,/path/to/processed/EGA_DATASET
        
        For --samples parameter (CSV file - dataset_id is optional):
        id,path,dataset_id
        SAMPLE1,/path/to/processed/SAMPLE1,GSE123456
        SAMPLE2,/path/to/processed/SAMPLE2,GSE123456
        SAMPLE3,/path/to/processed/SAMPLE3,

      iRODS Path Structure:
        - Datasets: uploaded to irodspath/id (e.g., /archive/cellgeni/sanger/GSE123456/)
        - Samples with dataset_id: uploaded to irodspath/dataset_id/id (e.g., /archive/cellgeni/sanger/GSE123456/SAMPLE1/)
        - Samples without dataset_id: uploaded to irodspath/id (e.g., /archive/cellgeni/sanger/SAMPLE1/)

      Pipeline workflow:
        1. Dataset/Sample Discovery - Reads dataset or sample information from CSV input file
        2. Public Dataset Detection - Identifies public datasets (GSE*, E-MTAB-*, PRJEB* patterns)
        3. Metadata Parsing - Extracts metadata from public repositories for public datasets
        4. Quality Control - Generates mapping QC statistics from STARsolo output (if not present)
        5. File Collection - Gathers all data files and metadata files for upload
        6. iRODS Upload - Transfers files to iRODS with checksums
        7. Metadata Attachment - Attaches comprehensive metadata to iRODS collections

      Examples:
        # Basic usage with datasets - Upload processed datasets to iRODS
        nextflow run main.nf --datasets datasets.csv --irodspath "/archive/cellgeni/sanger/"
        
        # Basic usage with individual samples
        nextflow run main.nf --samples samples.csv --irodspath "/archive/cellgeni/sanger/"
        
        # Custom ignore pattern for file filtering
        nextflow run main.nf --datasets datasets.csv --irodspath "/archive/cellgeni/sanger/" --ignore_pattern ".bam,.fastq.gz"
        
        # Disable public metadata collection
        nextflow run main.nf --datasets datasets.csv --irodspath "/archive/cellgeni/sanger/" --collect_public_metadata false
        
        # Enable verbose output
        nextflow run main.nf --datasets datasets.csv --irodspath "/archive/cellgeni/sanger/" --verbose true
        
        # Custom output directory
        nextflow run main.nf --datasets datasets.csv --irodspath "/archive/cellgeni/sanger/" --output_dir "my_results"
        
        # Dry run - test pipeline without uploading to iRODS
        nextflow run main.nf --datasets datasets.csv --irodspath "/archive/cellgeni/sanger/" --dry_run true

      Expected data structure:
        Each dataset/sample directory should contain:
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
    error("Please provide all required parameters: --datasets OR --samples AND --irodspath. See --help for more information.")
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
    // Read datasets list if provided
    datasets_list = params.datasets ? channel.fromPath(params.datasets) : channel.empty()
    datasets_list
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
    samples_list = params.samples ? channel.fromPath(params.samples) : channel.empty()
    samples_list
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
            def dataset_key = checkIfPublic(meta.dataset_id) ? "series" : "dataset_id"
            def metadata = [
                    id: meta.id,
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
            metadata[dataset_key] = meta.dataset_id
            [
                [id: meta.id, dataset_id: meta.dataset_id],
                metadata
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
    samples.collectFile(name: 'sample_metadata.csv', newLine: false, storeDir: params.output_dir, sort: true, keepHeader: true, skip: 1) { sample_meta, path, irodspath ->
            def meta_without_id = sample_meta.findAll { key, value -> key != 'id' }
            def meta_values = meta_without_id.values().join(',')
            def meta_keys = meta_without_id.keySet().join(',')
            def header = "sample_id,path,irodspath,${meta_keys}"
            def line = "${sample_meta.id},${path},${irodspath},${meta_values}"
            "${header}\n${line}\n"
            
        }
        .subscribe { __ -> 
            log.info("Sample metadata saved to ${params.output_dir}/sample_metadata.csv")
        }

    // Run iRODS upload workflow to upload sample directories to iRODS and attach metadata
    if (!params.dry_run) {
        ignore_pattern = params.ignore_pattern.split(',').collect { it.trim() }
        IRODS_UPLOAD_COLLECTION(
            samples,
            ignore_pattern
        )
    }

    // STEP 3: For each dataset upload metadata files to iRODS and attach metadata to dataset collections
    // Collect relevant metadata files for each dataset
   REPROCESSING_COLLECT_METADATA.out
        .mapping_metadata
        // remove mapping qc metadata for stand alone samples
        .filter { meta, _path -> !meta.relation || meta.relation.size() > 1 }
        // Attach public metadata to channel
        .mix(REPROCESSING_COLLECT_METADATA.out.public_metadata)
        // Create irodspath for each file
        .map { meta, path ->
            def unified_path = params.irodspath.replaceFirst('/$', '')
            def irodspath = "${unified_path}/${meta.id}/${path.name}"
            tuple([id: meta.id, path: path], path, irodspath)
        }
        // Save to a channel
        .tap { metadata_files }
        // Create a channel with metadata that will be attached to the collection
        .unique { meta, _path, _collection_irodspath ->  meta.id }
        .map { meta, _path, _collection_irodspath ->
            def dataset_key = checkIfPublic(meta.id) ? "study_accession_number" : "dataset_id"
            def collection_irodspath = "${params.irodspath.replaceFirst('/$', '')}/${meta.id}"
            def new_meta = [id: meta.id]
            new_meta[dataset_key] = meta.id
            tuple(new_meta, collection_irodspath)
        }
        .set { collection_metadata }
    
    // Run iRODS commmands if not in dry run mode
    if (!params.dry_run) {
        // Upload metadata files to iRODS
        IRODS_STOREFILE(metadata_files)

        // Collect versions of the tools used
        IRODS_UPLOAD_COLLECTION.out.md5
            .mix(IRODS_STOREFILE.out.md5)
            .collectFile(name: 'md5sums.csv', newLine: false, storeDir: params.output_dir, sort: true, keepHeader: true, skip: 1) { meta, irodspath, md5, md5irods -> 
                def header = "sample_id,filepath,irodspath,md5,irodsmd5"
                def line = "${meta.id},${meta.path},${irodspath},${md5},${md5irods}"
                "${header}\n${line}\n"
            }
            .subscribe { __ -> 
                log.info("MD5 checksums saved to ${params.output_dir}/md5sums.csv")
            }

        // Attach metadata to iRODS dataset collections
        IRODS_ATTACHCOLLECTIONMETA(collection_metadata)
    }

    // Collect dataset metadata to file
    collection_metadata.collectFile(name: 'dataset_metadata.csv', newLine: false, storeDir: params.output_dir, sort: true, keepHeader: true, skip: 1) { meta, irods_collection_path ->
            def meta_without_id = meta.findAll { key, value -> key != 'id' }
            def meta_values = meta_without_id.values().join(',')
            def meta_keys = meta_without_id.keySet().join(',')
            def header = "${meta_keys},irodspath"
            def line = "${meta_values},${irods_collection_path}"
            "${header}\n${line}\n"
        }
        .subscribe { __ -> 
            log.info("Dataset metadata saved to ${params.output_dir}/dataset_metadata.csv")
        }
}   