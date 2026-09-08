nextflow.enable.types = true

/*
 * Module: local/reprocess10x/validatelocal
 */

record Collection {
    id: String
    path: String
}

process REPROCESS10X_VALIDATELOCAL {
    tag "${id}"
    container 'quay.io/cellgeni/track-reprocessing:0.2.0'

    input:
    tuple(id: String, path: Path)
    schema: Path

    output:
    meta = tuple(id: id, path: path)
    txt: Path = file("${id}.txt")
    list: Path = file("extra_files.list")


    topic:
    tuple('validate-hierarchy', eval('validate-hierarchy --version')) >> 'versions'

    script:
    def args = task.ext.args ?: '--no-exit'
    """
    validate-hierarchy local \\
        "${path}" \\
        --schema ${schema} \\
        --report "${id}.txt" \\
        --extra-paths-file extra_files.list ${args}
    """
}
