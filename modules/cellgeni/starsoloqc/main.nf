/*
 * Module: cellgeni/starsoloqc
 */

process STARSOLOQC {
    tag "Collecting QC stats for ${meta.id}"

    container "quay.io/cellgeni/starsolo:v4.1"

    input:
    tuple val(meta), path(samples)

    output:
    tuple val(meta), path("${meta.id}.solo_qc.tsv"), emit: tsv
    path "versions.yml"           , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    starsolo qc */ $args > ${prefix}.solo_qc.tsv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        cellgeni/STARsolo: \$(starsolo --version)
    END_VERSIONS
    """

    stub:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    echo $args
    
    touch ${prefix}.solo_qc.tsv

    reprocess_version=\$(grep reprocess /versions.txt | cut -d ':' -f 2)
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        cellgeni/STARsolo: \$(starsolo --version)
    END_VERSIONS
    """
}
