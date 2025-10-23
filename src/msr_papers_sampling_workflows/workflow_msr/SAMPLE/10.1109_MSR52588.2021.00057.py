from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata

from sampling_mining_workflows_dsl.element.Loader import Loader
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

# ---- Metadata ----
url = Metadata.of_string("id")  # repository id
contain_correct_Dockerfile = Metadata.of_boolean("contain_correct_Dockerfile")


def main():
    workflow = (
        WorkflowBuilder()
        # Start from WOC distinct repositories
        .input(Loader(url, contain_correct_Dockerfile))
        # Keep only repositories containing a correct Dockerfile
        .add_metadata(Loader(url, contain_correct_Dockerfile))
        .filter_operator("contain_correct_Dockerfile")
        # Output the filtered repositories
        .output(CsvWriter("woc_repos_correct_dockerfile.csv"))
    )

    workflow.execute_workflow()
