# Data Augmentation for Supervised Code Translation Learning.
# * Filter for Java and C# projects
# * Remove projects part of the other dataset

from pathlib import Path

from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.element import Loader
from sampling_mining_workflows_dsl.operator.set_algebra.external_set_operator import DifferenceOperator
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder
from sampling_mining_workflows_dsl.operator.OperatorBuilder import OperatorBuilder
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata

def main():
    input_path = Path("input.json")
    url = Metadata.of_string("url")
    language = Metadata.of_string("language")

    #Execute 5 time the workflow
    for i in range(5):
        WorkflowBuilder()\
            .input(Loader("PGA dataset", url, language))\
            .add_metadata(Loader(url, language))\
            .filter_operator("language == 'Java' or language == 'C#'")\
            .difference_with_external_set_operator(Loader("java-c# dataset", url))\
            .output(CsvWriter(f"result{i}.csv"))