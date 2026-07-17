/*
 * Module: local/reprocess10x/aggregatemeta
 *
 * Aggregate all per-dataset metadata files (STARsolo QC + public accessions)
 * into a single per-sample CSV and JSON, keyed by the STARsolo sample name.
 */

process REPROCESS10X_AGGREGATEMETA {
    tag "${meta.id}"
    container 'quay.io/cellgeni/track-reprocessing:0.1.1'

    input:
    tuple val(meta), path(metafiles)

    output:
    tuple val(meta), path("${meta.id}.metadata.csv"),  emit: csv
    tuple val(meta), path("${meta.id}.metadata.json"), emit: json
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def suffix = task.ext.suffix ?: 'solo_qc'
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    aggregate_metadata.py \\
        ${metafiles} \\
        --out-csv "${prefix}.metadata.csv" \\
        --out-json "${prefix}.metadata.json" \\
        ${args}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version | cut -d ' ' -f 2)
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    touch "${prefix}.metadata.csv" "${prefix}.metadata.json"

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$(python3 --version | cut -d ' ' -f 2)
    END_VERSIONS
    """
}
