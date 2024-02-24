include { SPARK_STARTMANAGER   } from '../../../modules/janelia/spark/startmanager/main'
include { SPARK_WAITFORMANAGER } from '../../../modules/janelia/spark/waitformanager/main'
include { SPARK_STARTWORKER    } from '../../../modules/janelia/spark/startworker/main'
include { SPARK_WAITFORWORKER  } from '../../../modules/janelia/spark/waitforworker/main'

/**
 * Prepares a Spark context using either a distributed cluster or single process
 * as specified by the `spark_cluster` parameter.
 *
 * Expects a channel of meta tuples where the meta contains a `spark_work_dir` key.
 * A Spark cluster is created for each meta, and the Spark contexts are returned
 * appended to each meta tuple.
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

    // Create a Spark context for each meta
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

        // wait for all workers to start
        spark_cluster_res = SPARK_WAITFORWORKER(meta_workers).groupTuple(by: [0,1])
    }
    else {
        // When running locally, the driver needs enough resources to run a spark worker
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
