import importlib

from glue.context.context import args, job, logger

# Initialize the Glue job with the name provided in the command line arguments
job.init(args["JOB_NAME"], args)

# Retrieve the entrypoint and environment from the command line arguments
entrypoint = args["ENTRYPOINT"]
environment = args["ENV"]

# Log a welcome message indicating which entrypoint and environment are being used
logger.info(f"Welcome in entrypoint={entrypoint} for environment={environment}!")

# Dynamically construct the module name to import based on the entrypoint
module_name = f"glue.process.{entrypoint}"

# Import the module dynamically using importlib
entrypoint_module = importlib.import_module(module_name)

# Execute the run_job function from the imported module
entrypoint_module.run_job()

# Commit the job indicating successful completion
job.commit()
