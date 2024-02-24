//
// This file holds several functions specific to the main.nf workflow in the JaneliaSciComp/nf-n5-spark pipeline
//

import nextflow.Nextflow
import java.io.File

class WorkflowMain {

    //
    // Citation string for pipeline
    //
    public static String citation(workflow) {
        return "If you use ${workflow.manifest.name} for your analysis please cite:\n\n" +
            "* RS-FISH\n" +
            "  https://doi.org/10.1038/s41592-022-01669-y\n\n" +
            "* The nf-core framework\n" +
            "  https://doi.org/10.1038/s41587-020-0439-x\n\n" 
    }

    private static String checkPathParam(params, paramName) {
        def paramValue = params.get(paramName)
        def file = new File(paramValue)
        if (!file.exists()) {
            Nextflow.error("The path specified by --"+paramName+" does not exist: "+paramValue)
        }
        return file.toPath().toAbsolutePath().normalize().toString()
    }

    //
    // Validate parameters and print summary to screen
    //
    public static Map<String, Object> initialise(workflow, params, log) {

        // Print workflow version and exit on --version
        if (params.version) {
            String workflow_version = NfcoreTemplate.version(workflow)
            log.info "${workflow.manifest.name} ${workflow_version}"
            System.exit(0)
        }

        // Check that a -profile or Nextflow config has been provided to run the pipeline
        NfcoreTemplate.checkConfigProvided(workflow, log)

        // Check AWS batch settings
        NfcoreTemplate.awsBatch(workflow, params)

        if (params.spark_workers > 1 && !params.spark_cluster) {
            Nextflow.error("You must enable --spark_cluster if --spark_workers is greater than 1.")
        }

        def input_image = checkPathParam(params, "input_image")
        def outdir = checkPathParam(params, "outdir")

        def final_params = [
            'input_image': input_image,
            'outdir': outdir
        ]

        return final_params
    }
}
