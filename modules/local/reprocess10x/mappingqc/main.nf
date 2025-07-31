process REPROCESS10X_MAPPINGQC {
    tag "Collecting QC stats for $prefix"

    container "${ workflow.containerEngine == 'singularity' ? 'docker://quay.io/cellgeni/reprocess_10x:dev': 'quay.io/cellgeni/reprocess_10x:dev' }"

    input:
    tuple val(meta), path(dataset)

    output:
    tuple val(meta), path("${prefix}.solo_qc.tsv"), emit: tsv
    path "versions.yml"           , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    prefix = task.ext.prefix ?: "${meta.id}"
    """
    workdir=\$(pwd) 
    cd ${dataset}
    solo_QC.sh > \${workdir}/${prefix}.solo_qc.tsv

    reprocess_version=\$(grep reprocess /versions.txt | cut -d ':' -f 2)
    cat <<-END_VERSIONS > \${workdir}/versions.yml
    "${task.process}":
        cellgeni/reprocess_public_10x: \$reprocess_version
    END_VERSIONS
    """

    stub:
    def args = task.ext.args ?: ''
    prefix = task.ext.prefix ?: "${meta.id}"
    """
    echo $args
    
    touch ${prefix}.solo_qc.tsv

    reprocess_version=\$(grep reprocess /versions.txt | cut -d ':' -f 2)
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        cellgeni/reprocess_public_10x: \$reprocess_version
    END_VERSIONS
    """
}
