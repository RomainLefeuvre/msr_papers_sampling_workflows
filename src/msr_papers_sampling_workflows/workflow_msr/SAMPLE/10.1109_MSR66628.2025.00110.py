# * Use Islam et al PD dataset
# * random partition 80-20, 5 times 

from pathlib import Path

from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.element.loader import JsonLoader
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder
from sampling_mining_workflows_dsl.operator.OperatorBuilder import OperatorBuilder
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata

def main():
    input_path = Path("input.json")
    url = Metadata.of_string("url")

    #Execute 5 time the workflow
    for i in range(5):
        WorkflowBuilder()\
            .input(JsonLoader(input_path, url))\
            .grouping_operator(
                OperatorBuilder().random_selection_operator(80, 5)\
                                 .set_output_set_id("random_output_set"),
                OperatorBuilder().difference_with_operator("random_output_set")
            )\
            .output(CsvWriter(f"result{i}.csv"))
