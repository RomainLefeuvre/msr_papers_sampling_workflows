from pathlib import Path

from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.element.loader import JsonLoader
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

# GIRT-Model: Automated Generation of Issue Report Templates.

# * Retrieve the GIRT-Data dataset


def main():
    input_path = Path("girt-dataset.json")
    url = Metadata.of_string("url")

    # Workflow Declaration and Execution
    workflow = (
        WorkflowBuilder()
        .input(JsonLoader(input_path, url))
        .output(CsvWriter("out.csv"))
    )

    workflow.execute_workflow()


if __name__ == "__main__":
    main()
