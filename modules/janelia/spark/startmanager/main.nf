process SPARK_STARTMANAGER {
    label 'process_single'
    container 'docker.io/biocontainers/spark:3.1.3_cv1'

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
