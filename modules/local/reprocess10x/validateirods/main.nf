nextflow.enable.types = true
/*
 * Module: local/irods/validateirods
 */


process REPROCESS10X_VALIDATEIRODS {
    tag "${id}"
    container 'quay.io/cellgeni/track-reprocessing:0.2.0'

    input:
    tuple(id: String, irodspath: String)
    schema: Path
    config: Path?

    output:
    meta = tuple(id: id, path: irodspath)
    txt: Path = file("${id}.txt")
    list: Path = file("extra_files.list")

    topic:
    tuple('validate-hierarchy', eval('validate-hierarchy --version')) >> 'versions'

    script:
    def args = task.ext.args ?: '--no-exit'
    def irodsconfig = config ?: "~/.irods/irods_environment.json"
    """
    validate-hierarchy irods \\
        "${irodspath}" \\
        --schema ${schema} \\
        --config-file ${irodsconfig} \\
        --report "${id}.txt" \\
        --extra-paths-file extra_files.list ${args}
    """
}
