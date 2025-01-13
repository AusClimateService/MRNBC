# Bash script for sending jobs

# which step to start and finish at: (because often chunking does not need to be repeated if there was an error later)
# chunk = 1, calc = 2, fix = 3, convert = 4
start_from=1
finish_at=4


# quick function to add a depend trigger on previous job in step if there was one
get_depend () {
    if [[ $1 == "" ]]
    then
        echo ""
    else
        echo "-W depend=afterok:$1"
    fi
}


submit_jobs () {
    # create log directory structure
    dir_path="logs"
    timestamp=$(date +"%m_%d_%H-%M-%S")
    mkdir -p $dir_path
    
    job=""

    # chunk
    if [[ $start_from -le 1 ]] && [[ $finish_at -ge 1 ]]
    then
        job=$(qsub $(get_depend $job) -j oe -o "$dir_path/chunk-$timestamp.OU" job-chunk.pbs)
        echo $job
    fi

    # calc
    if [[ $start_from -le 2 ]] && [[ $finish_at -ge 2 ]]
    then
        job=$(qsub $(get_depend $job) -j oe -o "$dir_path/calc-$timestamp.OU" job-calc.pbs)
        echo $job
    fi

    # repair
    if [[ $start_from -le 3 ]] && [[ $finish_at -ge 3 ]]
    then
        job=$(qsub $(get_depend $job) -j oe -o "$dir_path/fix-$timestamp.OU" job-fix.pbs)
        echo $job
    fi

    # convert
    if [[ $start_from -le 4 ]] && [[ $finish_at -ge 4 ]]
    then
        job=$(qsub $(get_depend $job) -j oe -o "$dir_path/convert-$timestamp.OU" job-convert.pbs)
        echo $job
    fi
}

submit_jobs
