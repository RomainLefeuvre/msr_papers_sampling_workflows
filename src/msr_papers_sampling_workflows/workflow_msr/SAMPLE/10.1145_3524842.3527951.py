
from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata

from sampling_mining_workflows_dsl.element.Loader import Loader
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder


def main():
    url = Metadata.of_string("id")

    workflow = (
        WorkflowBuilder()
        .input(
            Loader(
                url,
            )
        )
        .output(CsvWriter("rodrigues.csv"))
    )

    workflow.execute_workflow()
