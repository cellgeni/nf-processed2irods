include { DATASETS2IRODS } from './subworkflows/local/datasets2irods'
include { REPROCESSING_COLLECT_METADATA } from './subworkflows/local/reprocessing_collect_metadata'

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
    def exclusions = ['raw', 'fastq', 'temp', 'log']
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
            tuple( meta, file(meta.path, type: 'dir') )}
        // .map { meta -> 
        //     // Remove null entries from metadata
        //     meta.findAll { key, value -> 
        //         value != null && value.toString().trim() != ""
        //     }
        // }
        // Save to a channel
        .tap { samples }
        // Group samples by dataset_id (if specified)
        .map { meta, path ->
            def dataset_id = meta.dataset_id ?: meta.id
            [
                [id: dataset_id],
                path
            ]
        }
        .groupTuple()
        .set { grouped_samples }
    
    // Combine sample channels
    samples = samples.mix(dataset_samples)
    // samples.view { meta, path ->
    //     "Sample ID: ${meta.id}, Path: ${path}, Dataset ID: ${meta.dataset_id}"
    // }
    // Collect metadata
    grouped_samples = grouped_samples.mix(grouped_dataset_samples)
    grouped_samples.view { meta, pathlist -> 
        "Dataset ID: ${meta.id}, Sample IDs: ${pathlist.collect { it.baseName }.join(', ')}"
    }
    REPROCESSING_COLLECT_METADATA(
      grouped_samples,
      params.collect_public_metadata,
      params.parse_mapper_metrics
    )
}