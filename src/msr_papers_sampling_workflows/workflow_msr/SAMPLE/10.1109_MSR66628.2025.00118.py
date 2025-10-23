# * Start from 5 MSR studies
# * Union of all sets 

from pathlib import Path

from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.element.loader import JsonLoader
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder
from sampling_mining_workflows_dsl.operator.OperatorBuilder import OperatorBuilder
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata

def main():
    input_path = Path("input.json")
    url = Metadata.of_string("url")

    study_1_path = "input_study_1.json"
    study_2_path = "input_study_2.json"
    study_3_path = "input_study_3.json"
    study_4_path = "input_study_4.json"
    study_5_path = "input_study_5.json"


    WorkflowBuilder()\
            .input(JsonLoader(study_1_path, url))\
            .union_with_external_set_operator(JsonLoader(study_2_path, url))\
            .union_with_external_set_operator(JsonLoader(study_3_path, url))\
            .union_with_external_set_operator(JsonLoader(study_4_path, url))\
            .union_with_external_set_operator(JsonLoader(study_5_path, url))\
            .output(CsvWriter(f"result.csv"))
