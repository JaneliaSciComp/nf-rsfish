include { SPARK_TERMINATE } from '../../../modules/janelia/spark/terminate/main'

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
