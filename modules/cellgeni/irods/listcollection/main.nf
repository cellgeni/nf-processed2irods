/*
 * Module: cellgeni/irods/listcollection
 */

process IRODS_LISTCOLLECTION {
    tag "${meta.id}"
    container 'community.wave.seqera.io/library/pip_python-irodsclient:b889a3a62a0b1462'

    input:
    tuple val(meta), val(irodspath)

    output:
    tuple val(meta), path("output.csv"), emit: csv
    path 'versions.yml'           , emit: versions

    script:
    def args = task.ext.args ?: ""
    """
    listcollection.py \\
        ${irodspath} \\
        -o output.csv \\
        ${args}
    
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        python: \$( python --version 2>&1 | awk '{print \$2}' )
        python-irodsclient: \$( python -c "import irods; print(irods.__version__)" )
    END_VERSIONS
    """
}
