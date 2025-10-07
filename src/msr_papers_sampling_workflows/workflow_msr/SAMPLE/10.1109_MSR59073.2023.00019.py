from pathlib import Path

from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.element.loader import JsonLoader
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

# AutoML from Software Engineering Perspective: Landscapes and Challenges.

# AutoML Framework identification :
# * Filter repos with 'automl' topic
# * Sort repos by stars and take 10 firsts
# * Manual check of description and documentation


# repositories on WoC :
# * Filter for repos that import one of the identified automl frameworks
# * Filter out forked repos


def main():
    input_path = Path("input.json")
    url = Metadata.of_string("url")

    topic = Metadata.of_string("topic")
    stars = Metadata.of_string("stars")

    imported_frameworks = Metadata.of_string("imported_frameworks")
    is_fork = Metadata.of_boolean("is_fork")

    # Workflow Declaration and Execution
    automl_fw_extraction = (
        WorkflowBuilder()
        .input(JsonLoader(input_path, url, topic, stars))
        .filter_operator("topic == 'automl'")
        .systematic_selection_operator(10, stars, 1)
        .manual_sampling_operator()
        .output(CsvWriter("out.csv"))
    )

    automl_fw_extraction.execute_workflow()
    # Identified AutoML frameworks from the paper in part III/1
    # Expected result from the first workflow
    extracted_frameworks = ["nni", "tpot", "autokeras", "auto-sklearn", "autogluon"]  # noqa: F841

    downstream_repos_workflow = (
        WorkflowBuilder()
        .input(JsonLoader(input_path, url, imported_frameworks, is_fork))
        .filter_operator("bool(set(imported_frameworks) & set(extracted_frameworks))")
        .filter_operator("not is_fork")
        .output(CsvWriter("downstream_repos.csv"))
    )
    downstream_repos_workflow.execute_workflow()


if __name__ == "__main__":
    main()
