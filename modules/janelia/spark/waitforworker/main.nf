process SPARK_WAITFORWORKER {
    label 'process_single'
    container 'docker.io/biocontainers/spark:3.1.3_cv1'
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
