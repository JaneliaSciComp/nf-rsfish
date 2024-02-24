process RS_FISH {
    tag "${meta.id}"
    container "ghcr.io/janeliascicomp/rs-fish-spark:8f8954f"
    cpus { spark.driver_cores }
    memory { spark.driver_memory }

    input:
    tuple val(meta),
          path(input_image),
          val(input_dataset),
          val(spark)

    output:
    tuple val(meta),
          path(input_image),
          val(input_dataset),
          val(spark), 
          emit: params
    path output_filename, emit: csv
    path "versions.yml", emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    extra_args = task.ext.args ?: ''
    output_filename = meta.id + "-points.csv"
    executor_memory = spark.executor_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    driver_memory = spark.driver_memory.replace(" KB",'k').replace(" MB",'m').replace(" GB",'g').replace(" TB",'t')
    """
    INPUT_N5=\$(realpath ${input_image})
    /opt/scripts/runapp.sh "$workflow.containerEngine" "$spark.work_dir" "$spark.uri" \
        /app/app.jar net.preibisch.rsfish.spark.SparkRSFISH \
        $spark.parallelism $spark.worker_cores "$executor_memory" $spark.driver_cores "$driver_memory" \
        --image=\$INPUT_N5 --dataset=${input_dataset} \
        --output=${output_filename} \
        ${extra_args}

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        spark: \$(cat /opt/spark/VERSION)
        rs-fish-spark: \$(cat /app/VERSION)
    END_VERSIONS
    """
}
