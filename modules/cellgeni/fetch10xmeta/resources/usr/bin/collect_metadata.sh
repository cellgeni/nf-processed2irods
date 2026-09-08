#!/bin/bash 

set -uo pipefail

function download_geo_family() {
  local SERIES=$1

  ## download the so-called soft_family file, and use it to generate same files as above
  local PAD=`echo $SERIES | perl -ne 's/\d{3}$/nnn/; print'`
  wget --tries=5 --wait=10 --retry-connrefused --retry-on-http-error=503 -O ${SERIES}_family.soft.gz https://ftp.ncbi.nlm.nih.gov/geo/series/$PAD/$SERIES/soft/${SERIES}_family.soft.gz
  ## -f overwrites the old stuff
  gzip -fd ${SERIES}_family.soft.gz
  if [[ ! -s ${SERIES}_family.soft ]]
  then
    >&2 echo "ERROR: Failed to download ${SERIES}_family.soft file; please make sure the series you requested exists, or fix the download URL!"
    exit 1
  fi
}

function download_sdrf_idf_files() {
  local SERIES=$1

  wget --tries=5 --wait=10 --retry-connrefused -O $SERIES.sdrf.txt https://www.ebi.ac.uk/biostudies/files/$SERIES/$SERIES.sdrf.txt
  wget --tries=5 --wait=10 --retry-connrefused -O $SERIES.idf.txt https://www.ebi.ac.uk/biostudies/files/$SERIES/$SERIES.idf.txt
  
  if [[ ! -s $SERIES.sdrf.txt ]] 
  then
    >&2 echo "ERROR: Failed to download $SERIES.sdrf.txt file; please make sure the series you requested exists, or fix the download URL!"
    exit 1
  fi
}

## derive the sample and biosample lists from the relation table. '-' marks a
## relation we do not have; it must never reach the lists, both because it is
## not an ID anyone can look up and because a lone '-' used as a grep -f
## pattern matches every line of a metadata table.
function write_sample_lists() {
  local SERIES=$1

  cut -f 2 $SERIES.sample.relation.list | grep -v '^-$' | grep . > $SERIES.sample.list
  cut -f 4 $SERIES.sample.relation.list | grep -v '^-$' | grep . > $SERIES.biosample.list
  return 0
}

## true when the family file did not give us an SRA experiment and/or a BioSample
## for every GSM it lists
function relations_incomplete() {
  local SERIES=$1

  if [[ ! -s $SERIES.sample.relation.list ]]
  then
    return 0
  fi

  if [[ -s $SERIES.gsm.list && `cat $SERIES.gsm.list | wc -l` -ne `cat $SERIES.sample.relation.list | wc -l` ]]
  then
    return 0
  fi

  cut -f 3,4 $SERIES.sample.relation.list | grep -q -- '-'
}

## Fill in the sample relations the family.soft file failed to provide, using the
## metadata tables we have already downloaded.
##
## GEO does not always record '!Sample_relation = SRA: ...' (and occasionally not
## the BioSample either) — GSE135325 and GSE137444 list the BioSample alone —
## which used to leave the relation table, and with it the sample and biosample
## lists, completely empty. The runinfo/filereport tables hold exactly the same
## relations: every run carries its GSM, its SRX and its BioSample on one line.
## So we group those lines by GSM and rebuild the missing fields from them.
##
## Fields are found by their shape rather than by column number, so the same
## pass works on both the SRA table (GSM in col 30, SRX 11, SAMN 26) and the ENA
## one (GSM in col 7, SRX 6, SAMN 4).
function recover_sample_relations() {
  local SERIES=$1
  shift

  local METAS=()
  local META
  for META in "$@"
  do
    if [[ -s $META ]]
    then
      METAS+=("$META")
    fi
  done

  if [[ ${#METAS[@]} -eq 0 ]]
  then
    >&2 echo "ERROR: Cannot recover sample relations for $SERIES: no metadata table available!"
    return 1
  fi

  if [[ ! -s $SERIES.gsm.list ]]
  then
    >&2 echo "ERROR: Cannot recover sample relations for $SERIES: no GSM IDs found in ${SERIES}_family.soft!"
    return 1
  fi

  ## the relation table is read back in, so whatever the family file *did* tell
  ## us always wins over the tables
  touch $SERIES.sample.relation.list

  awk -F'\t' -v OFS='\t' \
      -v GSMS="$SERIES.gsm.list" -v REL="$SERIES.sample.relation.list" '
    FILENAME == GSMS { if ($1 != "") { order[++n] = $1; want[$1] = 1 } next }

    FILENAME == REL {
      if ($2 == "" || !want[$2]) next
      if ($3 != "-") sra[$2] = $3
      if ($4 != "-") bio[$2] = $4
      next
    }

    ## one metadata row: pick out the GSM it belongs to and the IDs beside it.
    ## a row that names no GSM is still indexed by its BioSample, which is how a
    ## sample gets resolved when the family file gave us that and nothing else
    {
      gsm = ""; srx = ""; samn = ""
      for (i = 1; i <= NF; i++) {
        if      ($i ~ /^GSM[0-9]+$/ && want[$i]) gsm  = $i
        else if ($i ~ /^[SED]RX[0-9]+$/)         srx  = $i
        else if ($i ~ /^SAM[NED][A-Z]?[0-9]+$/)  samn = $i
      }
      if (gsm != "") {
        seen[gsm] = 1
        add(gsm, srx)
        if (samn != "" && bio[gsm] == "") bio[gsm] = samn
      }
      else if (samn != "") {
        seen_samn[samn] = 1
        if (srx != "") samn_sra[samn] = samn_sra[samn] "," srx
      }
    }

    ## a GSM split over several experiments keeps all of them, comma separated
    function add(g, x) {
      if (x == "" || sra[g] ~ ("(^|,)" x "(,|$)")) return
      sra[g] = (sra[g] == "" ? x : sra[g] "," x)
    }

    END {
      for (j = 1; j <= n; j++) {
        g = order[j]

        if (!seen[g] && bio[g] != "" && seen_samn[bio[g]]) {
          seen[g] = 1
          split(substr(samn_sra[bio[g]], 2), xs, ",")
          for (k in xs) add(g, xs[k])
        }

        ## nothing in either table knows this sample, so nothing downstream could
        ## look it up either: the accession lookup would fail on it and take the
        ## whole series down with it. Series routinely mix in samples that are not
        ## in SRA at all (GSE135325 lists four BD AbSeq samples next to its two 10x
        ## ones), so leave it out and carry on
        if (!seen[g]) {
          print "WARNING: no SRA/ENA metadata found for sample " g "; leaving it out" > "/dev/stderr"
          continue
        }

        print g, g, (sra[g] == "" ? "-" : sra[g]), (bio[g] == "" ? "-" : bio[g])
        kept++
      }
      if (kept == 0)
        print "ERROR: none of the " n " samples of the series were found in the metadata tables!" > "/dev/stderr"
    }
  ' "$SERIES.gsm.list" "$SERIES.sample.relation.list" "${METAS[@]}" > $SERIES.sample.relation.list.tmp

  if [[ ! -s $SERIES.sample.relation.list.tmp ]]
  then
    >&2 echo "ERROR: Failed to recover any sample relations for $SERIES from ${METAS[*]}!"
    rm -f $SERIES.sample.relation.list.tmp
    return 1
  fi

  mv $SERIES.sample.relation.list.tmp $SERIES.sample.relation.list
  write_sample_lists $SERIES

  ## samples missing from the tables have been left out by now, so only the
  ## relations of the samples we kept are worth complaining about
  if cut -f 3,4 $SERIES.sample.relation.list | grep -q -- '-'
  then
    >&2 echo "WARNING: Some sample relations for $SERIES are still incomplete after reading ${METAS[*]}; the accessions will be looked up by whichever ID we do have."
  fi

  >&2 echo "Recovered relations for `cat $SERIES.sample.relation.list | wc -l` of `cat $SERIES.gsm.list | wc -l` samples of $SERIES."
  return 0
}

function parse_geo_family() {
  local SERIES=$1

  ## get bioproject ID
  grep Series_relation ${SERIES}_family.soft | perl -ne 'print "$1\n" if (m/(PRJ[A-Z]+\d+)/)' | sort | uniq  > $SERIES.project.list

  if [[ ! -s $SERIES.project.list ]]
  then
    >&2 echo "WARNING: No project ID found in ${SERIES}_family.soft file! Trying e-utils method..."
    GEOID=$(curl -g --retry 5 --retry-delay 1 --fail --silent \
    "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi?db=gds&term=${SERIES}[ACCN]+GSE[ETYP]&retmode=json" \
    | jq -r ".esearchresult.idlist[0] // empty" 2>/dev/null)

    curl -g --retry 5 --retry-delay 1 --fail --silent \
      "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi?db=gds&id=$GEOID&retmode=json" \
      | jq -r ".result.\"$GEOID\".bioproject // empty" 2>/dev/null \
      | grep PRJ > $SERIES.project.list
  fi
  
  ## the full list of GSM IDs the family file declares, in the order it declares
  ## them; kept separately because the relation table below may not manage a line
  ## for every one of them, and recover_sample_relations needs to know what is missing
  grep '^\^SAMPLE' ${SERIES}_family.soft | perl -ne 'print "$1\n" if (m/(GSM\d+)/)' > $SERIES.gsm.list

  ## get sample IDs; samples here are GSM IDs; usually for a 10x GSM==SRS==SRX, but I haven't checked *all* of the SRA you know
  ##
  ## One line per ^SAMPLE record, with '-' standing in for a relation the file
  ## does not carry. Accumulating the fields until all four are set instead
  ## would drop every sample with a missing relation *and* leave the fields of
  ## that sample set, so the next sample's SRX would be paired with the previous
  ## sample's BioSample. Missing fields are filled in later from the SRA/ENA
  ## metadata tables (see recover_sample_relations).
  awk '
  BEGIN {OFS="\t"}

  function flush() {
    if (sample != "")
      print sample, (geo == "" ? sample : geo), (sra == "" ? "-" : sra), (biosample == "" ? "-" : biosample)
    sample=""; geo=""; sra=""; biosample=""
  }

  /\^SAMPLE/ { flush(); match($0, /GSM[0-9]+/); sample=substr($0, RSTART, RLENGTH) }
  /Sample_geo_accession/ { match($0, /GSM[0-9]+/); geo=substr($0, RSTART, RLENGTH) }
  /Sample_relation = SRA:/ { match($0, /SRX[0-9]+/); sra=substr($0, RSTART, RLENGTH) }
  /Sample_relation = BioSample:/ { match($0, /SAMN[0-9]+/); biosample=substr($0, RSTART, RLENGTH) }

  END { flush() }
  ' ${SERIES}_family.soft > $SERIES.sample.relation.list

  write_sample_lists $SERIES

  ## first variable is used to spot dbGap and other problematic datasets;
  local EXPIDS=`grep Series_relation ${SERIES}_family.soft | grep -v PRJ | wc -l`
  
  ## few sanity checks:
  if [[ `cat $SERIES.project.list | wc -l` -gt 1 ]]
  then 
    >&2 echo "WARNING: more than 1 project associated with series $SERIES! This shouldn't normally happen, do take a look."
  fi 

  if [[ $EXPIDS == "0" ]]
  then
    >&2 echo "WARNING: No secondary run/experiment (SRP/SRX) IDs in the family.soft file; this often happens in datasets that are restricted access (dbGap, etc)."
  fi
}

function parse_sdrf_idf() {
  local SERIES=$1

  ## samples are ERS in case of ArrayExpress. Why not ERX, you might ask? Yes, ask you might.   
  cat $SERIES.sdrf.txt | tr '\t' '\n' | grep "^ERS" | sort | uniq > $SERIES.sample.list
}

function get_subseries_from_family {
  local SERIES=$1
  local OUTPUT_FILE=$2
  local SUBGSE=`grep Series_relation ${SERIES}_family.soft | grep SuperSeries | perl -ne 'print "$1\n" if (m/(GSE\d+)/)'`
  
  ## delete output file if it exists
  if [[ -f $OUTPUT_FILE ]]
  then
    rm $OUTPUT_FILE
  fi

  ## pulls sub-series data
  if [[ $SUBGSE == "" ]]
  then
    >&2 echo "ERROR: No GSE subseries were listed in ${SERIES}_family.soft file!"
    return 1
  else
    for i in $SUBGSE
    do
      local PAD=`echo $i | perl -ne 's/\d{3}$/nnn/; print'`
      wget --tries=5 --wait=10 --retry-connrefused --retry-on-http-error=503 -O ${i}_family.soft.gz https://ftp.ncbi.nlm.nih.gov/geo/series/$PAD/$i/soft/${i}_family.soft.gz
      gzip -fd ${i}_family.soft.gz
      grep Series_relation ${i}_family.soft | perl -ne 'print "$1\n" if (m/(PRJ[A-Z]+\d+)/)' | sort | uniq >> $OUTPUT_FILE
    done
  fi
  return 1
}

function download_metadata {
  local SERIES=$1
  local SCRIPT=$2
  local DOWNLOAD_LIST=$3
  local OUTPUT_FILE=$4

  local STATUS=1
  local TRIES=1

  if [[ ! -s $DOWNLOAD_LIST ]]
  then
    >&2 echo "ERROR: No download list $DOWNLOAD_LIST found!"
    return 1
  fi


  while [[ ! $STATUS -eq 0 && TRIES -le 5 ]]
  do
    $SCRIPT $DOWNLOAD_LIST > $OUTPUT_FILE
    STATUS=$?
    TRIES=$((TRIES+1))
    sleep 1
  done

  if [[ ! -s $OUTPUT_FILE ]]
  then
    >&2 echo "ERROR: Failed to download metadata for $SERIES using $SCRIPT and $DOWNLOAD_LIST!"
    return 1
  else
    return $STATUS
  fi
}

function write_accessions() {
  local SERIES=$1
  local SAMPLE=$2
  local SMPS=$3
  local EXPS=$4
  local RUNS=$5

  ## check that we have all the IDs
  if [[ $EXPS == "" || $RUNS == "" ]]
  then
    return 1
  fi

  # write the accessions to the accessions file
  if [[ $SERIES == GSE* ]]
  then
    echo -e "$SAMPLE\t$SMPS\t$EXPS\t$RUNS" >> $SERIES.accessions.tsv
  else
    echo -e "-\t$SAMPLE\t$EXPS\t$RUNS" >> $SERIES.accessions.tsv
  fi
  return 0
}

function get_sample_ids() {
  local SERIES=$1
  local META=$2
  local STATUS=1

  ## delete accessions file if exists
  if [[ -s $SERIES.accessions.tsv ]] 
  then
    rm $SERIES.accessions.tsv
  fi

  ## get sample, experiment, and run IDs for each sample from metadata file
  if [[ -s $META ]]
  then
    for i in `cat $SERIES.sample.list`
    do
      ## get BioSample ID if possible
      if [[ -s $SERIES.sample.relation.list ]]
      then
        local biosample=`grep $i $SERIES.sample.relation.list | cut -f 4 | tr -d '\n'`
      else
        local biosample=$i
      fi
      
      ## try to get sample, experiment, and run IDs from metadata file using GSM
      if [[ `grep $i $META` ]]
      then
        SMPS=`grep $i $META | tr '\t' '\n' | grep -P "^[SED]RS\d+$" | sort | uniq | tr '\n' ',' | sed "s/,$//"`
        EXPS=`grep $i $META | tr '\t' '\n' | grep -P "^[SED]RX\d+$" | sort | uniq | tr '\n' ',' | sed "s/,$//"`
        RUNS=`grep $i $META | tr '\t' '\n' | grep -P "^[SED]RR\d+$" | sort | uniq | tr '\n' ',' | sed "s/,$//"`
        write_accessions $SERIES $i $SMPS $EXPS $RUNS
        STATUS=$?
      ## try to get sample, experiment, and run IDs from metadata file using BioSample
      elif [[ `grep $biosample $META` ]]
      then
        SMPS=`grep $biosample $META | tr '\t' '\n' | grep -P "^[SED]RS\d+$" | sort | uniq | tr '\n' ',' | sed "s/,$//"`
        EXPS=`grep $biosample $META | tr '\t' '\n' | grep -P "^[SED]RX\d+$" | sort | uniq | tr '\n' ',' | sed "s/,$//"`
        RUNS=`grep $biosample $META | tr '\t' '\n' | grep -P "^[SED]RR\d+$" | sort | uniq | tr '\n' ',' | sed "s/,$//"`
        write_accessions $SERIES $i $SMPS $EXPS $RUNS
        STATUS=$?
      else
        >&2 echo "ERROR: No experiment or run ID found for $i in $META!"
        STATUS=1
        break
      fi

      ## check that we have all the IDs
      if [[ $STATUS -eq 1 ]]
      then
        >&2 echo "WARNING: No experiment or run ID found for $i in $META!"
        break
      fi
    done

    ## check that all samples are in accessions file and change status to 0.
    ## an empty sample list is never a success: it used to compare 0 with 0 and
    ## report everything was fine, leaving the failure to surface much later as
    ## a missing run list
    if [[ -s $SERIES.accessions.tsv && `cat $SERIES.sample.list | wc -l` -eq `cut -f 1 $SERIES.accessions.tsv | wc -l` ]]
    then
      STATUS=0
    fi
  else
    >&2 echo "ERROR: No metadata file $META found!"
  fi
  return $STATUS
}

subset_sample_list() {
  local SERIES=$1
  local SUBSET=${2:-""}

  if [[ -s $SUBSET ]]
  then
    >&2 echo "Narrowing down the dataset using the file $SUBSET"
    >&2 echo "New list of the samples to be processed:"
    >&2 cat $SUBSET
    ## add newline character to the end of the file if there is none
    sed -i -e '$a\' $SUBSET
    ## subset the sample list
    grep -f $SUBSET $SERIES.sample.list > $SERIES.sample.list.tmp
    mv $SERIES.sample.list.tmp $SERIES.sample.list
  fi
}

subset_accessions() {
  local SERIES=$1
  local SUBSET=${2:-""}

  if [[ -s $SUBSET ]]
  then
    ## subset the accessions file
    grep -f $SUBSET $SERIES.accessions.tsv > $SERIES.accessions.tsv.tmp
    mv $SERIES.accessions.tsv.tmp $SERIES.accessions.tsv
  fi
}

subset_meta() {
  local META=$1
  local SUBSET=${2:-""}

  if [[ -s $SUBSET ]]
  then
    grep -f $SUBSET $META > $META.tmp
    mv $META.tmp $META
  fi
}

## narrow one metadata table down to the requested samples, preferring the
## caller's subset and falling back to the BioSample IDs when the table is keyed
## by those instead. A missing table is not an error: only one of ENA/SRA may exist.
subset_metadata_table() {
  local SERIES=$1
  local META=$2
  local SUBSET=${3:-""}

  if [[ ! -s $META ]]
  then
    return 0
  fi

  if [[ -s $SUBSET ]] && grep -q -f $SUBSET $META
  then
    subset_meta $META $SUBSET
  elif [[ -s "$SERIES.biosample.list" ]]
  then
    subset_meta $META $SERIES.biosample.list
  else
    >&2 echo "WARNING: No subset file provided, and no biosample list found; using the full metadata file $META"
  fi
}

function make_run_relation_files() {
  local SERIES=$1

  ## make run list
  cut -f 4 $SERIES.accessions.tsv | tr ',' '\n' | sort | uniq > $SERIES.run.list
  
  ## make sample x run file
  if [[ $SERIES == GSE* ]]
  then
    cut -f 1,4 $SERIES.accessions.tsv > $SERIES.sample_x_run.tsv
  else
    cut -f 2,4 $SERIES.accessions.tsv > $SERIES.sample_x_run.tsv
  fi                            
}

function make_util_files() {
  local SERIES=$1
  local SUBSET=${2:-""}
  local STATUS=1

  if [[ -s $SERIES.accessions.tsv ]]
  then
    >&2 echo "WARNING: file $SERIES.accessions.tsv exists. This shouldn't normally happen. Overwriting the file.."
    rm $SERIES.accessions.tsv
  fi

  ## narrow down the sample list before looking up accessions, so samples outside the
  ## requested subset (which may be missing from the metadata) don't abort the whole run
  subset_sample_list $SERIES $SUBSET

  if [[ ! -s $SERIES.sample.list ]]
  then
    >&2 echo "ERROR: No samples to process for $SERIES! Either no sample IDs could be worked out from the metadata, or none of them matched the requested subset."
    exit 1
  fi

  ## get sample, experiment, and run IDs for each sample from SRA metadata file
  if [[ $SERIES == GSE* ]]
  then
    get_sample_ids $SERIES $SERIES.sra.tsv
    STATUS=$?
  fi

  ## get sample, experiment, and run IDs for each sample from ENA metadata file
  if [[ $STATUS -eq 1 ]]
  then
    get_sample_ids $SERIES $SERIES.ena.tsv
    STATUS=$?
  fi

  if [[ $STATUS -eq 1 ]]
  then
    >&2 echo "ERROR: Failed to get sample, experiment, and run IDs for $SERIES using any of the available metadata files!"
    exit 1
  fi

  ## subset the accessions file if a sample list is provided
  subset_accessions $SERIES $SUBSET

  ## make few more useful metadata files
  make_run_relation_files $SERIES

  ## finally, classify each run into 3 major types:
  ## 1) we have useable 10x paired-end files; 2) we need to get them from 10x BAM; 3) we need to get them from SRA
  ## simultaneously, '$SERIES.urls.list' is generated listing all things that need to be downloaded
  if [[ ! -s "$SERIES.ena.tsv" && ! -s "$SERIES.sra.tsv" ]]
  then
    >&2 echo "ERROR: No metadata file found for $SERIES!"
    exit 1
  fi

  ## both tables are narrowed down and handed to the parser together: a run is
  ## frequently listed in only one of them, and the species recorded in the
  ## other is the only thing standing between us and an 'UNKNOWN'
  subset_metadata_table $SERIES $SERIES.ena.tsv $SUBSET
  subset_metadata_table $SERIES $SERIES.sra.tsv $SUBSET

  parse_metadata.sh $SERIES > $SERIES.parsed.tsv
}

function process_geo() {
  local SERIES=$1
  local SUBSET=${2:-""}
  local SRA_STATUS=1
  local ENA_STATUS=1

  ## download the family file from GEO
  download_geo_family $SERIES
  
  ## parse the family file to get the project and sample IDs
  parse_geo_family $SERIES

  ## Try loading metadata using $SERIES.project.list
  if [[ -s $SERIES.project.list ]]
  then
    ## download metadata from SRA
    download_metadata "$SERIES" "curl_sra_metadata.sh" "$SERIES.project.list" "$SERIES.sra.tsv"
    SRA_STATUS=$?

    ## download metadata from ENA
    download_metadata "$SERIES" "curl_ena_metadata.sh" "$SERIES.project.list" "$SERIES.ena.tsv"
    ENA_STATUS=$?
  fi


  ## if the download failed, try using suboroject IDs
  if [ $SRA_STATUS -eq 1 ] || [ $ENA_STATUS -eq 1 ]
  then
    >&2 echo "WARNING: replacing $SERIES.project.list with sub-series projects.."
    ## get subseries from family file
    get_subseries_from_family "$SERIES" "$SERIES.subproject.list"

    ## download metadata from SRA
    if [ $SRA_STATUS -eq 1 ]
    then
      download_metadata "$SERIES" "curl_sra_metadata.sh" "$SERIES.subproject.list" "$SERIES.sra.tsv"
      SRA_STATUS=$?
    fi

    ## download metadata from ENA
    if [ $ENA_STATUS -eq 1 ]
    then
      download_metadata "$SERIES" "curl_ena_metadata.sh" "$SERIES.subproject.list" "$SERIES.ena.tsv"
      ENA_STATUS=$?
    fi
  fi


## if the download using subproject IDs failed, try using BioSample IDs
  if [ $SRA_STATUS -eq 1 ] || [ $ENA_STATUS -eq 1 ]
  then
    >&2 echo "WARNING: replacing $SERIES.subproject.list with BioSample IDs.."

    ## download metadata from SRA
    if [ $SRA_STATUS -eq 1 ]
    then
      download_metadata "$SERIES" "curl_sra_metadata.sh" "$SERIES.biosample.list" "$SERIES.sra.tsv"
      SRA_STATUS=$?
    fi

    ## download metadata from ENA
    if [ $ENA_STATUS -eq 1 ]
    then
      download_metadata "$SERIES" "curl_ena_metadata.sh" "$SERIES.biosample.list" "$SERIES.ena.tsv"
      ENA_STATUS=$?
    fi
  fi

  ## if both downloads failed, exit with an error
  if [ $SRA_STATUS -eq 1 ] && [ $ENA_STATUS -eq 1 ]
  then
    >&2 echo "ERROR: Failed to download metadata for $SERIES using any of the available methods!"
    exit 1
  fi

  ## the family file does not always carry the SRA (and sometimes not even the
  ## BioSample) relation of its samples; now that we have the metadata tables,
  ## take the missing relations from there
  if relations_incomplete $SERIES
  then
    >&2 echo "WARNING: ${SERIES}_family.soft did not give a complete set of sample relations; recovering them from the metadata tables.."
    recover_sample_relations $SERIES $SERIES.sra.tsv $SERIES.ena.tsv
    if [[ $? -ne 0 ]]
    then
      >&2 echo "ERROR: Failed to recover sample relations for $SERIES!"
      exit 1
    fi
  fi

  ## make utility files
  make_util_files $SERIES $SUBSET
}

function process_arrayexpress {
  local SERIES=$1
  local SUBSET=${2:-""}

  ## download the SDRF and IDF files from ArrayExpress
  download_sdrf_idf_files $SERIES
  
  ## parse the SDRF file to get the project and sample IDs
  parse_sdrf_idf $SERIES

  ## download metadata from ENA
  download_metadata "$SERIES" "curl_ena_metadata.sh" "$SERIES.sample.list" "$SERIES.ena.tsv"
  local ENA_STATUS=$?

  ## if failed, exit with an error
  if [ $ENA_STATUS -eq 1 ]
  then
    >&2 echo "ERROR: Failed to download metadata for $SERIES using any of the available methods!"
    exit 1
  fi

  ## make utility files
  make_util_files $SERIES $SUBSET
}

function process_bioproject {
  local SERIES=$1
  local SUBSET=${2:-""}
  
  ## simple version of GEO processing (see above): pull all the needed metadata from ENA using PRJ*
  echo $SERIES > $SERIES.project.list

  ## download metadata from ENA
  download_metadata "$SERIES" "curl_ena_metadata.sh" "$SERIES.project.list" "$SERIES.ena.tsv"
  local ENA_STATUS=$?

  ## if failed, exit with an error
  if [ $ENA_STATUS -eq 1 ]
  then
    >&2 echo "ERROR: Failed to download metadata for $SERIES using any of the available methods!"
    exit 1
  fi

  ## create sample list
  cat $SERIES.ena.tsv | tr '\t' '\n' | grep -P "^[SED]RS\d+$" | sort | uniq > $SERIES.sample.list 

  ## make utility files
  make_util_files $SERIES $SUBSET
}

function main () {
  if (( $# != 1 && $# != 2 ))
  then
    >&2 echo "USAGE: collect_metadata.sh <series_id> [sample_list]"
    >&2 echo
    >&2 echo "(requires curl_ena_metadata.sh, curl_sra_metadata.sh and parse_metadata.sh present in the same directory)"
    exit 1
  fi

  local SERIES=$1
  local SUBSET=${2:-""}

  # Handle different series types
  case "$SERIES" in
    GSE*)  process_geo "$SERIES" "$SUBSET" ;;
    E-MTAB*) process_arrayexpress "$SERIES" "$SUBSET" ;;
    PRJ*)  process_bioproject "$SERIES" "$SUBSET" ;;
    *) echo "ERROR: The series ID must start with GSE, E-MTAB, or PRJ!" >&2; exit 1 ;;
  esac
}

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
