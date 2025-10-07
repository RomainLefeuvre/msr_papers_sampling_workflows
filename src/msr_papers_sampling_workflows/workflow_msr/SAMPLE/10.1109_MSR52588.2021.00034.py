from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata

from sampling_mining_workflows_dsl.element.Loader import Loader
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

# ---- Metadata ----
url = Metadata.of_string("id")
presence_of_flaky_annotation = Metadata.of_boolean("presence_of_flaky_annotation")
flaky_nb = Metadata.of_integer("flaky_nb")


def main():
    workflow = (
        WorkflowBuilder()
        # Start from Sourcegraph
        .input(Loader(url, presence_of_flaky_annotation, flaky_nb))
        # Keep projects that have flaky annotation
        .filter_operator("presence_of_flaky_annotation")
        # Keep projects with more than 30 flaky annotations
        .filter_operator("flaky_nb > 30")
        # Output final dataset
        .output(CsvWriter("sourcegraph_flaky.csv"))
    )

    workflow.execute_workflow()
