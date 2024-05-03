import importlib

from glue.context.context import args, job, logger

job.init(args["JOB_NAME"], args)

entrypoint = args["ENTRYPOINT"]
environment = args["ENV"]
logger.info(f"Welcome in entrypoint={entrypoint} for environment={environment} !")
module_name = f"glue.process.{entrypoint}"
entrypoint_module = importlib.import_module(module_name)
entrypoint_module.run_job()

job.commit()
