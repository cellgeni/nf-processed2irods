/*
 * Module: local/irods/validateirods
 */

process REPROCESS10X_VALIDATEIRODS {
    tag "${meta.id}"
    container 'quay.io/cellgeni/track-reprocessing:0.1.1'

    input:
    tuple val(meta), val(irodspath)
    path irodsconfig

    output:
    tuple val(meta), path("*.txt"), emit: txt
    path "versions.yml", emit: versions

    script:
    def args = task.ext.args ?: '--no-exit'
    """
    sample-tracking irods-validate "${irodspath}" --report "${meta.id}.txt" --config-file ${irodsconfig} ${args}
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        sample-tracking: \$(sample-tracking --version | cut -d ' ' -f 2)
    END_VERSIONS
    """
}
