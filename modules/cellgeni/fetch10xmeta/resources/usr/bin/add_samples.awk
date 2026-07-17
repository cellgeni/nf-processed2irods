#!/usr/bin/env -S awk -f

BEGIN {
    FS = "\t"
    OFS = "\t"
}

# First file: sample_x_run.tsv
NR == FNR {
    # Split the second column (runs) by commas into an array.
    n = split($2, runs, /,/)
    for(i = 1; i <= n; i++) {
        mapping[runs[i]] = $1
    }
    next
}

# Second file: parsed.tsv
{
    run = $1
    sample = (run in mapping ? mapping[run] : "NA")
    print $0, sample
}