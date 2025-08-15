include { IRODS_STOREFILE } from '../../../modules/local/irods/storefile'
include { IRODS_ATTACHCOLLECTIONMETA } from '../../../modules/local/irods/attachcollectionmeta'


def matchFileName(path, ignore_ext) {
    return !ignore_ext.any { ext -> path.name.contains(ext) }
}

workflow IRODS_UPLOAD_COLLECTION {

    take:
    directories     // channel: [ val(meta), originalpath, irodspath ]
    ignore_pattern  // list: file extensions to ignore

    main:
    // STEP 1: Upload directories to iRODS
    // Get all file paths in the directories
    files = directories
        .map { meta, path, irodsdir ->
            def unified_path = path.toString().replaceFirst('/$', '')
            [
                [id: meta.id, original_dir: unified_path],
                irodsdir,
                file(unified_path + '/**', type: 'file')
            ]
        }
        // flatten file lists [meta, [file1, file2, ...]] -> [meta, file1], [meta, file2], ...
        .transpose()
        // Attach iRODS path to each file
        .map { meta, irodsdir, path ->
            def relativepath = path.toString().replaceFirst(meta.original_dir, '').replaceFirst('/', '')
            def irodspath = irodsdir.replaceFirst('/$', '') + "/${relativepath}"
            tuple( [id: meta.id, path: path], path, irodspath )
        }
    
    // Filter files if ignore_pattern is provided
    if ( ignore_pattern ) {
        files = files.filter { meta, path, irodspath ->
            matchFileName(path, ignore_pattern)
        }
    }

    IRODS_STOREFILE(files)

    // STEP 2: Attach metadata to iRODS collection
    // Create metadata channel
    metadata = directories.map { meta, path, irodsdir -> tuple( meta, irodsdir ) }
    IRODS_ATTACHCOLLECTIONMETA(metadata)

    // Collect versions of the tools used
    versions = IRODS_STOREFILE.out.versions.mix(
        IRODS_ATTACHCOLLECTIONMETA.out.versions
    )
    emit:
    md5 = IRODS_STOREFILE.out.md5
    versions = versions
}
