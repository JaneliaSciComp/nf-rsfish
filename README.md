# RS-FISH Nextflow Pipeline

Nextflow pipeline wrapper for [PreibischLab/RS-FISH-Spark](https://github.com/PreibischLab/RS-FISH-Spark) which implements "precise, interactive, fast, and scalable FISH spot detection." This pipeline allows you to easily run RS-FISH at scale on any infrastructure supported by Nextflow. 

## Quick Start

The only software requirements for running this pipeline are [Nextflow](https://www.nextflow.io) (version 20.10.0 or greater) and either Docker or [Singularity](https://sylabs.io) (version 3.5 or greater). If you are running in an HPC cluster, ask your system administrator to install Singularity on all the cluster nodes.

To [install Nextflow](https://www.nextflow.io/docs/latest/getstarted.html):

    curl -s https://get.nextflow.io | bash 

Alternatively, you can install it as a conda package:

    conda create --name nextflow -c bioconda nextflow

To [install Singularity](https://sylabs.io/guides/3.7/admin-guide/installation.html) on CentOS Linux:

    sudo yum install singularity

Now you can run the pipeline and it will download everything else it needs. The following command will analyze one input image in N5 format and save a CSV of detected spots to the `./output` directory. 

    nextflow run JaneliaSciComp/nf-rsfish --input_image /path/to/image.n5 \
        --input_dataset /c0/s0 --outdir ./output -profile singularity

By default, the pipeline runs a local Java process. To scale to larger data sizes, you can run with a Spark cluster, e.g.

    nextflow run JaneliaSciComp/nf-rsfish --input_image /path/to/image.n5 \
        --input_dataset /c0/s0 --outdir ./output -profile janelia \
        --spark_cluster=true --spark_workers=2 --spark_worker_cores=4 --spark_gb_per_core=15


## Interactive Parameter Finding

You can use the RS-FISH Fiji Plugin to interactively find the correct parameters for your data as follows.

1. Use File->Import->N5 to open your data set and select the "Crop" option to cut out a small but interesting region for testing spot detection.
2. File->Save As->Tiff... and reopen the TIFF image.
3. Install RS-FISH plugins: Help->Update... then click "Manage Update Sites" and enable "Radial Symmetry" then click Close and restart Fiji. 
4. Plugins -> RS-FISH -> Tools -> Calculate Anisotropy Coefficient. Save this coefficient for later use. It should stay consistent when using the same microscope.
5. Plugins -> RS-FISH -> RS-FISH 
    * Set the anisotropy coefficient from step 4
    * Click Ok and Done on the options dialog
    * Set the minimum value in the intensity distribution
    * Click "OK press to proceed to final results"
6. RS-FISH -> Tools -> Show Detections in BigDataViewer

Iteratively adjust the parameters until you are detecting all the spots you want.

At the end, you can choose RS-FISH -> Advanced to view all of parameter values, or record to a macro to save them. This will give you all the parameters you need to run the pipeline:

```
run("RS-FISH", "image=cropped.tif mode=Advanced anisotropy=0.7213 robust_fitting=RANSAC compute_min/max use_anisotropy sigma=1.50000 threshold=0.00500 support=3 min_inlier_ratio=0.10 max_error=1.50 spot_intensity_threshold=269.54 background=[No background subtraction] background_subtraction_max_error=0.05 background_subtraction_min_inlier_ratio=0.10 results_file=[]");
```
