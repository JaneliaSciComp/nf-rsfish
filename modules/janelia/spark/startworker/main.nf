process SPARK_STARTWORKER {
    container 'docker.io/biocontainers/spark:3.1.3_cv1'
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
