from pathlib import Path

from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.element.loader.JsonLoader import JsonLoader
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata

from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder


def main():
    # ---- Metadata ----
    url = Metadata.of_string("id")
    workflow = (
        WorkflowBuilder()
        .input(
            JsonLoader(
                Path("android_time_machine.json"),
                url,
            )
        )
        .output(CsvWriter("out_1.csv"))
    )

    workflow.execute_workflow()

    # ---- Metadata ----
    url = Metadata.of_string("id")
    workflow = (
        WorkflowBuilder()
        .input(
            JsonLoader(
                Path("CodeSearch-Net_Java_Dataset"),
                url,
            )
        )
        .output(CsvWriter("out_2.csv"))
    )

    workflow.execute_workflow()



