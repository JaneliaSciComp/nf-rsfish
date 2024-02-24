process SPARK_STARTMANAGER {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:3.1.3'

    input:
    tuple val(meta), val(spark)

    output:
    tuple val(meta), val(spark)

    when:
    task.ext.when == null || task.ext.when

    script:
    args = task.ext.args ?: ''
    spark_local_dir = task.ext.spark_local_dir ?: "/tmp/spark-${workflow.sessionId}"
    sleep_secs = task.ext.sleep_secs ?: '1'
    spark_config_filepath = "${spark.work_dir}/spark-defaults.conf"
    spark_master_log_file = "${spark.work_dir}/sparkmaster.log"
    terminate_file_name = "${spark.work_dir}/terminate-spark"
    container_engine = workflow.containerEngine
    """
    /opt/scripts/startmanager.sh "$spark_local_dir" "${spark.work_dir}" "$spark_master_log_file" \
        "$spark_config_filepath" "$terminate_file_name" "$args" $sleep_secs $container_engine
    """
}

process SPARK_WAITFORMANAGER {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:3.1.3'
    errorStrategy { task.exitStatus == 2
        ? 'retry' // retry on a timeout to prevent the case when the waiter is started before the master and master never gets its chance
        : 'terminate' }
    maxRetries 20

    input:
    tuple val(meta), val(spark)

    output:
    tuple val(meta), val(spark), env(spark_uri)

    when:
    task.ext.when == null || task.ext.when

    script:
    sleep_secs = task.ext.sleep_secs ?: '1'
    max_wait_secs = task.ext.max_wait_secs ?: '3600'
    spark_master_log_name = "${spark.work_dir}/sparkmaster.log"
    terminate_file_name = "${spark.work_dir}/terminate-spark"
    """
    /opt/scripts/waitformanager.sh "$spark_master_log_name" "$terminate_file_name" $sleep_secs $max_wait_secs
    export spark_uri=`cat spark_uri`
    """
}

process SPARK_STARTWORKER {
    container 'ghcr.io/janeliascicomp/spark:3.1.3'
    cpus { spark.worker_cores }
    memory { spark.worker_memory }

    input:
    tuple val(meta), val(spark), val(worker_id)
    path(data_dir)

    output:
    tuple val(meta), val(spark), val(worker_id)

    when:
    task.ext.when == null || task.ext.when

    script:
    args = task.ext.args ?: ''
    sleep_secs = task.ext.sleep_secs ?: '1'
    spark_worker_log_file = "${spark.work_dir}/sparkworker-${worker_id}.log"
    spark_config_filepath = "${spark.work_dir}/spark-defaults.conf"
    terminate_file_name = "${spark.work_dir}/terminate-spark"
    worker_memory = spark.worker_memory.replace(" KB",'').replace(" MB",'').replace(" GB",'').replace(" TB",'')
    container_engine = workflow.containerEngine
    """
    /opt/scripts/startworker.sh "${spark.work_dir}" "${spark.uri}" $worker_id \
        ${spark.worker_cores} ${worker_memory} \
        "$spark_worker_log_file" "$spark_config_filepath" "$terminate_file_name" \
        "$args" $sleep_secs $container_engine
    """
}

process SPARK_WAITFORWORKER {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:3.1.3'
    // retry on a timeout to prevent the case when the waiter is started
    // before the worker and the worker never gets its chance
    errorStrategy { task.exitStatus == 2 ? 'retry' : 'terminate' }
    maxRetries 20

    input:
    tuple val(meta), val(spark), val(worker_id)

    output:
    tuple val(meta), val(spark), val(worker_id)

    when:
    task.ext.when == null || task.ext.when

    script:
    sleep_secs = task.ext.sleep_secs ?: '1'
    max_wait_secs = task.ext.max_wait_secs ?: '3600'
    spark_worker_log_file = "${spark.work_dir}/sparkworker-${worker_id}.log"
    terminate_file_name = "${spark.work_dir}/terminate-spark"
    """
    /opt/scripts/waitforworker.sh "${spark.uri}" \
        "$spark_worker_log_file" "$terminate_file_name" \
        $sleep_secs $max_wait_secs
    """
}

process SPARK_CLEANUP {
    label 'process_single'
    container 'ghcr.io/janeliascicomp/spark:3.1.3'

    input:
    tuple val(meta), val(spark), val(worker_ids)

    output:
    tuple val(meta), val(spark), val(worker_ids)

    script:
    """
    find ${spark.work_dir} -name app.jar -exec rm {} \\;
    """
}

/**
 * Prepares a Spark context using either a distributed cluster or single process
 * as specified by the `spark_cluster` parameter.
 */
workflow SPARK_START {
    take:
    ch_meta               // channel: [ val(meta) ]
    working_dir           // path: shared storage path for worker communication
    data_dirs             // path: [ array of paths to be mounted to the Spark workers ]
    spark_cluster         // boolean: use a distributed cluster?
    spark_workers         // int: number of workers in the cluster (ignored if spark_cluster is false)
    spark_worker_cores    // int: number of cores per worker
    spark_gb_per_core     // int: number of GB of memory per worker core
    spark_driver_cores    // int: number of cores for the driver
    spark_driver_memory   // string: memory specification for the driver

    main:

    // create a Spark context for each meta
    def meta_and_sparks = ch_meta.map {
        def meta = it[0]
        def spark_work_dir = "${working_dir}/spark/${workflow.sessionId}/${meta.id}" 
        file(spark_work_dir).mkdirs()
        def spark = [:]
        spark.work_dir = spark_work_dir
        spark.workers = spark_workers ?: 1
        spark.worker_cores = spark_worker_cores
        spark.driver_cores = spark_driver_cores ?: 1
        spark.driver_memory = spark_driver_memory
        spark.parallelism = (spark_workers * spark_worker_cores)
        // 1 GB of overhead for Spark, the rest for executors
        spark.worker_memory = (spark_worker_cores * spark_gb_per_core + 1)+" GB"
        spark.executor_memory = (spark_worker_cores * spark_gb_per_core)+" GB"
        [meta, spark]
    }

    if (spark_cluster) {
        // start the Spark manager
        // this runs indefinitely until SPARK_TERMINATE is called
        SPARK_STARTMANAGER(meta_and_sparks)

        // start a watcher that waits for the manager to be ready
        SPARK_WAITFORMANAGER(meta_and_sparks)

        // cross product all worker directories with all worker numbers
        def worker_list = 1..spark_workers
        def meta_workers = SPARK_WAITFORMANAGER.out.map {
            def (meta, spark, spark_uri) = it
            spark.uri = spark_uri
            [meta, spark]
        }
        | combine(worker_list)

        // start workers
        // these run indefinitely until SPARK_TERMINATE is called
        SPARK_STARTWORKER(meta_workers, data_dirs)

        // when workers exit they should clean up after themselves
        SPARK_CLEANUP(SPARK_STARTWORKER.out.groupTuple(by: [0,1]))

        // wait for all workers to start
        spark_cluster_res = SPARK_WAITFORWORKER(meta_workers).groupTuple(by: [0,1])
    }
    else {
        // when running locally, the driver needs enough resources to run a spark worker
        spark_cluster_res = meta_and_sparks.map {
            def (meta, spark) = it
            spark.workers = 1
            spark.driver_cores = spark_driver_cores + spark_worker_cores
            spark.driver_memory = (2 + spark_worker_cores * spark_gb_per_core) + " GB"
            spark.uri = 'local[*]'
            [meta, spark]
        }
    }

    spark_context = spark_cluster_res.map {
        def (meta, spark) = it
        log.debug "Created Spark context for ${meta.id}: "+spark
        [meta, spark]
    }

    emit:
    spark_context // channel: [ val(meta), val(spark) ]
}
