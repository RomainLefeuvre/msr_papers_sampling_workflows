
from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.element.Loader import Loader
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

# ---- Metadata ----
url = Metadata.of_string("id")
referenced_on_f_droid = Metadata.of_boolean("referenced_on_f_droid")

def main():
    workflow = (
        WorkflowBuilder()
        .input(Loader(url))
        # Add referenced_on_f_droid metadata
        .add_metadata(Loader("referenced_on_f_droid", referenced_on_f_droid))
        # Keep only repositories referenced on F-Droid
        .filter_operator("referenced_on_f_droid")
        # Output final dataset
        .output(CsvWriter("github_fdroid.csv"))
    )

    workflow.execute_workflow()
