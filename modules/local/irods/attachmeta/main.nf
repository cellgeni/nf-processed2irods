def metaToTsv(meta) {
    tsv_string = meta
                     .findAll { key, value -> key != 'id' } //drop 'id' key
                     .collectMany { key, value ->
                         value.toString()
                              .split(/\s*,\s*/) // split by comma
                              .collect { it.trim() } // trim whitespace
                              .findAll { it } // filter out empty strings
                              .collect { v -> "${key}\t${v}" } // create key-value pairs
                    }
                    .join('\n')
                    .stripIndent() // remove leading whitespace
    return tsv_string
}

process IRODS_ATTACHMETA {
    tag "Attaching metadata for $prefix"

    input:
    tuple val(meta), path(irodspath)

    output:
    path "versions.yml"           , emit: versions

    script:
    def args = task.ext.args ?: ''
    prefix = task.ext.prefix ?: "${meta.id}"
    meta_tsv = metaToTsv(meta)
    """
    # Create tsv file with metadata
    echo -e "$meta_tsv" > metadata.tsv

    # If object is a collection, create a directory for it

    # Get existing metadata from iRODS
    imeta ls ${task.ext.type} "$irodspath" > existing_metadata.txt

    # Load metadata to iRODS
    while IFS=\$'\\t' read -r key value; do
        [[ -z "\$key" || -z "\$value" ]] && continue  # skip empty lines

        # Check if the key already exists in iRODS metadata
        if grep -qzP "attribute: $key\nvalue: $value" existing_metadata.txt; then
            echo "[SKIP] \$key=\$value already present"
        else
            echo "Adding \$key=\$value to iRODS metadata"
            imeta add ${task.ext.type} "$irodspath" "$key" "$value"
        fi
    done < metadata.tsv

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        irods: \$(ienv | grep version | awk '{ print \$3 }')
    END_VERSIONS
    """

    stub:
    def args = task.ext.args ?: ''
    prefix = task.ext.prefix ?: "${meta.id}"
    """
    echo $args
    
    touch ${prefix}.bam

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        irods: \$(ienv | grep version | awk '{ print \$3 }')
    END_VERSIONS
    """
}
