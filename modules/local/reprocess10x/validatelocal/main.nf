/*
 * Module: local/reprocess10x/validatelocal
 */

process REPROCESS10X_VALIDATELOCAL {
    tag "${meta.id}"
    container 'quay.io/cellgeni/track-reprocessing:0.1.1'

    input:
    tuple val(meta), path(samples, stageAs: "dataset/*")

    output:
    tuple val(meta), path(samples), emit: dataset
    tuple val(meta), path("*.txt"), emit: txt
    path "versions.yml", emit: versions

    script:
    def args = task.ext.args ?: ''
    """
    ln -s dataset "${meta.id}"
    sample-tracking local-validate "${meta.id}" --report "${meta.id}.txt" --follow-symlinks ${args}
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        sample-tracking: \$(sample-tracking --version | cut -d ' ' -f 2)
    END_VERSIONS
    """
}
