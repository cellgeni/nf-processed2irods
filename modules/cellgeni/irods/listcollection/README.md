# cellgeni/irods/listcollection

## Summary

Lists the contents of an iRODS collection and writes the results to a CSV file with columns: `type`, `path`, `size`, `checksum`.

The module:

1. Connects to iRODS using the environment file at `~/.irods/irods_environment.json` (created by `iinit`).
2. Lists data objects and subcollections at the given iRODS path.
3. Writes results to `output.csv`.

By default the listing is non-recursive. Pass `--recursive` via `ext.args` to recurse into subcollections.

## Inputs

| Name | Type | Description |
|---|---|---|
| `meta.id` | string | Sample identifier. |
| `irodspath` | string | iRODS collection path to list, e.g. `/archive/cellgeni/myproject`. |

## Outputs

| Name | File | Description |
|---|---|---|
| `csv` | `output.csv` | CSV file listing collection contents (type, path, size, checksum). |
| `versions` | `versions.yml` | Software version record. |

## Prerequisites

The iRODS environment file must be available at `~/.irods/irods_environment.json` inside the container. The easiest way is to run `iinit` on the host and add the bind mounts to the global Singularity `runOptions` in `nextflow.config`:

```groovy
singularity {
    runOptions = '-B /lustre,/nfs,/etc/ssl -B /home/USER/.irods:/root/.irods'
}
```

Replace `/home/USER` with your actual home directory path.

If the iRODS server requires SSL (`"irods_client_server_policy": "CS_NEG_REQUIRE"` in `irods_environment.json`), the `/etc/ssl` bind is required so the container can find the host's CA certificates.

Alternatively, pass a custom env file path via `ext.args`:

```groovy
withName: ".*IRODS_LISTCOLLECTION" {
    ext.args = "--env-file /path/to/irods_environment.json"
}
```

## Configuration via `ext` variables

All module behaviour is controlled through `task.ext` in `nextflow.config`. Example:

```groovy
singularity {
    runOptions = '-B /lustre,/nfs,/etc/ssl -B /home/USER/.irods:/root/.irods'
}

process {
    withName: ".*IRODS_LISTCOLLECTION" {
        ext.args = ""
        queue    = 'normal'
        cpus     = 1
        memory   = '4.GB'
    }
}
```

### Optional `ext` variables

| Variable | Type | Default | Description |
|---|---|---|---|
| `ext.args` | string | `""` | Additional arguments passed to `listcollection.py`. See below. |

### `ext.args` — additional arguments

| Argument | Description |
|---|---|
| `--recursive` | Recurse into subcollections. |
| `--env-file <path>` | Path to `irods_environment.json`. Overrides the default `~/.irods/irods_environment.json`. |

## Usage

```nextflow
include { IRODS_LISTCOLLECTION } from 'cellgeni/irods/listcollection'

IRODS_LISTCOLLECTION(
    channel.of([[id: 'myproject'], '/archive/cellgeni/myproject'])
)
```

## License

MIT
