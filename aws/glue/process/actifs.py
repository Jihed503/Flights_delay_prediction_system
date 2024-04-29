from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from ..common.s3 import write_to_parquet
from ..config.config import app_config
from ..data.actifs.actifs_schema import ACTIFS_SCHEMA
from ..data.fiag_fipl_attached.fiag_fipl_attached_reader import (
    FiagFiplAttachedReader,
)
from ..data.flights.fiag_fipl_attached_schema import N_FIN_PLAN
from ..data.fipl_financial_plan.fipl_financial_plan_reader import (
    FiplFinancialPlanReader,
)
from reporting_tool.data.fipl_financial_plan.fipl_financial_plan_schema import (
    C_IDENT_VAL,
)
from reporting_tool.data.rnat_ref_nat.rnat_ref_nat_reader import RnatRefNatReader
from reporting_tool.data.rnat_ref_nat.rnat_ref_nat_schema import C_IDENT_NATURE
from reporting_tool.data.valu_valuation.valu_valuation_reader import ValuValuationReader


class ActifsJob:
    def __init__(
        self,
        fipl_financial_plan_input_path: str,
        fiag_fipl_attached_input_path: str,
        rnat_ref_nat_input_path: str,
        valu_valuation_input_path: str,
        actifs_output_path: str,
    ) -> None:

        self.fipl_financial_plan_input_path: str = fipl_financial_plan_input_path
        self.fiag_fipl_attached_input_path: str = fiag_fipl_attached_input_path
        self.rnat_ref_nat_input_path: str = rnat_ref_nat_input_path
        self.valu_valuation_input_path: str = valu_valuation_input_path
        self.actifs_output_path: str = actifs_output_path

    def run(self) -> None:
        fipl_financial_plan_df: DataFrame = self._get_data_from_fipl_financial_plan(
            self.fipl_financial_plan_input_path
        )
        fiag_fipl_attached_df: DataFrame = self._get_data_from_fiag_fipl_attached(
            self.fiag_fipl_attached_input_path
        )

        valu_valuation_df: DataFrame = self._get_data_from_valu_valuation(
            self.valu_valuation_input_path
        )
        rnat_ref_nat_df: DataFrame = self._get_data_from_rnat_ref_nat(
            self.rnat_ref_nat_input_path
        )
        dataset_actifs: DataFrame = self._create_dataset_actifs(
            fipl_financial_plan_df,
            fiag_fipl_attached_df,
            valu_valuation_df,
            rnat_ref_nat_df,
        )

        self._write_dataset_actifs_to_s3(dataset_actifs, self.actifs_output_path)

    def _get_data_from_fipl_financial_plan(self, path: str) -> DataFrame:
        fipl_financial_plan_reader: FiplFinancialPlanReader = FiplFinancialPlanReader(
            path
        )
        fipl_financial_plan = fipl_financial_plan_reader.read()
        return fipl_financial_plan

    def _get_data_from_fiag_fipl_attached(self, path: str) -> DataFrame:
        fiag_fipl_attached_reader: FiagFiplAttachedReader = FiagFiplAttachedReader(path)
        fiag_fipl_attached = fiag_fipl_attached_reader.read()
        return fiag_fipl_attached

    def _get_data_from_valu_valuation(self, path: str) -> DataFrame:
        valu_valuation_reader: ValuValuationReader = ValuValuationReader(path)
        valu_valuation = valu_valuation_reader.read()
        return valu_valuation

    def _get_data_from_rnat_ref_nat(self, path: str) -> DataFrame:
        rnat_ref_nat_reader: RnatRefNatReader = RnatRefNatReader(path)
        rnat_ref_nat = rnat_ref_nat_reader.read()
        return rnat_ref_nat

    def _create_dataset_actifs(
        self,
        fipl_financial_plan_df: DataFrame,
        fiag_fipl_attached_df: DataFrame,
        valu_valuation_df: DataFrame,
        rnat_ref_nat_df: DataFrame,
    ) -> DataFrame:
        
        actifs_df = (
            (
                fiag_fipl_attached_df.join(
                    fipl_financial_plan_df, [C_IDENT_VAL, N_FIN_PLAN]
                )
                .join(valu_valuation_df, C_IDENT_VAL)
                .join(rnat_ref_nat_df, C_IDENT_NATURE)
            )
            .distinct()
            .select(*ACTIFS_SCHEMA.fieldNames())
        )
        return actifs_df

    def _write_dataset_actifs_to_s3(self, df: DataFrame, output_path: str) -> None:
        write_to_parquet(df, output_path)


def run_job(**kwargs: Any) -> None:
    print(f"Running Job with arguments[{kwargs}]")

    bucket_name_datalake = app_config.bucket_name_datalake
    bucket_name_results = app_config.bucket_name_results

    file_path_fipl_financial_plan = app_config.file_path_fipl_financial_plan
    file_path_fiag_fipl_attached = app_config.file_path_fiag_fipl_attached
    file_path_rnat_ref_nat = app_config.file_path_rnat_ref_nat
    file_path_valu_valuation = app_config.file_path_valu_valuation

    output_file_path_actifs = app_config.output_file_path_actifs
    date: str = datetime.now().strftime("%Y%m%d")
    fipl_financial_plan_path: str = (
        f"s3a://{bucket_name_datalake}/{file_path_fipl_financial_plan}"
    )
    fiag_fipl_attached_path: str = (
        f"s3a://{bucket_name_datalake}/{file_path_fiag_fipl_attached}"
    )
    rnat_ref_nat_path: str = f"s3a://{bucket_name_datalake}/{file_path_rnat_ref_nat}"
    valu_valuation_path: str = (
        f"s3a://{bucket_name_datalake}/{file_path_valu_valuation}"
    )
    actifs_output_path: str = (
        f"s3://{bucket_name_results}/{output_file_path_actifs}/eventdate={date}"
    )

    job: ActifsJob = ActifsJob(
        fipl_financial_plan_path,
        fiag_fipl_attached_path,
        rnat_ref_nat_path,
        valu_valuation_path,
        actifs_output_path,
    )
    job.run()
