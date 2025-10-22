from pathlib import Path
from sampling_mining_workflows_dsl.operator.OperatorBuilder import OperatorBuilder
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

from sampling_mining_workflows_dsl.element.Loader import Loader
from datetime import datetime
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata
from sampling_mining_workflows_dsl.element.writer.JsonWriter import JsonWriter
from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter

from sampling_mining_workflows_dsl.element.loader.JsonLoader import JsonLoader
from sampling_mining_workflows_dsl.Workflow import Workflow

from sampling_mining_workflows_dsl.analysis.HistWorkflowAnalysis import (
    HistWorkflowAnalysis,
)
from sampling_mining_workflows_dsl.exec_visualizer.WorkflowVisualizer import WorkflowVisualizer

# ---- Metadata ----
url = Metadata.of_string("url")
latest_commit_date = Metadata.of_date("latest_commit_date")
number_of_commits = Metadata.of_integer("number_of_commits")
number_of_commiters = Metadata.of_integer("number_of_commiters")
# ---- Workflow ----

workflow = WorkflowBuilder()\
    .input(JsonLoader(Path("test.json"),url,latest_commit_date,number_of_commits,number_of_commiters))\
    .filter_operator(latest_commit_date.is_after(datetime(2023, 1, 1)))\
    .grouping_operator(
        # First stratum: projects with less than 5 authors
        (
            OperatorBuilder(Workflow())
            .filter_operator("number_of_commiters < 5")
            .random_selection_operator(10000)
        ),
        # Second stratum: projects with 5 or more authors
        (
            OperatorBuilder(Workflow())
            .filter_operator("number_of_commiters >=5")
            .random_selection_operator(10000)
        ),
    )\
    .union_operator()\
    .output(JsonWriter("sampled_repos.json"))\
    .execute_workflow()\
    .print()
    

HistWorkflowAnalysis(metadata=number_of_commiters,top_x=-1,category=False,sort=True,output_path="./out").analyze(workflow)
WorkflowVisualizer(workflow,"./out" ).generate_graph()