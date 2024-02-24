process SPARK_TERMINATE {
    label 'process_single'
    container 'docker.io/biocontainers/spark:3.1.3_cv1'

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
