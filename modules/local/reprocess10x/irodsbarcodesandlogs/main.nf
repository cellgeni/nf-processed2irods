/*
 * Module: local/reprocess10x/irodsbarcodesandlogs
 */

process REPROCESS10X_IRODSBARCODESANDLOGS {
    tag "${meta.id}"

    input:
    tuple val(meta), val(irodsample)

    output:
    tuple val(meta), path("${meta.id}"), emit: sample
    path "versions.yml", emit: versions

    script:
    """
    mkdir -p ${meta.id}/output/Gene/filtered
    mkdir -p ${meta.id}/output/GeneFull
    iget -K -X retry.txt --retries 3 "${irodsample}/Log.out" "${meta.id}/Log.out"
    iget -K -X retry.txt --retries 3 "${irodsample}/Log.final.out" "${meta.id}/Log.final.out"
    iget -K -X retry.txt --retries 3 "${irodsample}/output/Gene/filtered/barcodes.tsv.gz" "${meta.id}/output/Gene/filtered/barcodes.tsv.gz"
    iget -K -X retry.txt --retries 3 "${irodsample}/output/Gene/Summary.csv" "${meta.id}/output/Gene/Summary.csv"
    iget -K -X retry.txt --retries 3 "${irodsample}/output/GeneFull/Summary.csv" "${meta.id}/output/GeneFull/Summary.csv"
    rm -f retry.txt
    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        irods: \$(ienv | grep version | awk '{ print \$3 }')
    END_VERSIONS
    """
}
