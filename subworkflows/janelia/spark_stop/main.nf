process SPARK_TERMINATE {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:3.1.3'

    input:
    tuple val(meta), val(spark)

    output:
    tuple val(meta), val(spark)

    script:
    terminate_file_name = "${spark.work_dir}/terminate-spark"
    """
    /opt/scripts/terminate.sh "$terminate_file_name"
    """
}

/**
 * Terminate the specified Spark clusters.
 */
workflow SPARK_STOP {
    take:
    ch_meta               // channel: [ val(meta), val(spark) ]
    spark_cluster         // boolean: use a distributed cluster?

    main:
    if (spark_cluster) {
        done = SPARK_TERMINATE(ch_meta)
    }
    else {
        done = ch_meta
    }

    emit:
    done
}
