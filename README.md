# RS-FISH Spark on Nextflow

Nextflow pipeline wrapper for [PreibischLab/RS-FISH-Spark](https://github.com/PreibischLab/RS-FISH-Spark). Allows you to easily run RS-FISH at scale on any infrastructure supported by Nextflow. 

## Quick Start

The only software requirements for running this pipeline are [Nextflow](https://www.nextflow.io) (version 20.10.0 or greater) and either Docker or [Singularity](https://sylabs.io) (version 3.5 or greater). If you are running in an HPC cluster, ask your system administrator to install Singularity on all the cluster nodes.

To [install Nextflow](https://www.nextflow.io/docs/latest/getstarted.html):

    curl -s https://get.nextflow.io | bash 

Alternatively, you can install it as a conda package:

    conda create --name nextflow -c bioconda nextflow

To [install Singularity](https://sylabs.io/guides/3.7/admin-guide/installation.html) on CentOS Linux:

    sudo yum install singularity

Now you can run the pipeline and it will download everything else it needs:

    nextflow run . --input_image /path/to/image.n5 --input_dataset /c0/s0 --outdir ./output -profile singularity

This will run RS-FISH as a local process and produce an `image.n5-points.csv` file in the `./output` directory. 

To run with a Spark cluster on the Janelia cluster:

    nextflow run . --input_image /path/to/image.n5 --input_dataset /c0/s0 --outdir ./output -profile janelia --spark_cluster=true --spark_workers=2 --spark_worker_cores=4 --spark_gb_per_core=15
