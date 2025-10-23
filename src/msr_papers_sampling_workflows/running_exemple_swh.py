from pathlib import Path
from sampling_mining_workflows_dsl.operator.OperatorBuilder import OperatorBuilder
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

from sampling_mining_workflows_dsl.element.Loader import Loader
from datetime import datetime
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata
from sampling_mining_workflows_dsl.element.writer.JsonWriter import JsonWriter
from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter

from sampling_mining_workflows_dsl.swh.loader import *
from sampling_mining_workflows_dsl.Workflow import Workflow

from sampling_mining_workflows_dsl.analysis.HistWorkflowAnalysis import (
    HistWorkflowAnalysis,
)
from sampling_mining_workflows_dsl.analysis.KSWorkflowAnalysis import KSWorkflowAnalysis
from sampling_mining_workflows_dsl.exec_visualizer.WorkflowVisualizer import WorkflowVisualizer



workflow = WorkflowBuilder()\
    .input(SwhLoader(swh_id,swh_commit_count,swh_commiter_count,swh_latest_commit_date))\
    .filter_operator(swh_latest_commit_date.is_after(datetime(2023, 1, 1)))\
    .grouping_operator(
        # First stratum: projects with less than 5 authors
        (
            OperatorBuilder(Workflow())
            .filter_operator("swh_commiter_count < 5")
            .random_selection_operator(10000,seed=42)
        ),
        # Second stratum: projects with 5 or more authors
        (
            OperatorBuilder(Workflow())
            .filter_operator("swh_commiter_count >=5")
            .random_selection_operator(10000, seed=42)
        ),
    )\
    .union_operator()\
    .output(JsonWriter("sampled_repos.json"))\
    .execute_workflow()\
    .print()
    
WorkflowVisualizer(workflow,"analysis/workflow_visualization").generate_graph()
HistWorkflowAnalysis(metadata=swh_commit_count,top_x=-1,category=False,sort=True,output_path="./analysis/hist/out_truncated",log_y=True,max_x_bound=3000000,fixed_bins=20).analyze(workflow)
HistWorkflowAnalysis(metadata=swh_commit_count,top_x=-1,category=False,sort=True,output_path="./analysis/hist/out_without_limit",log_y=True,fixed_bins=20).analyze(workflow)
KSWorkflowAnalysis(metadata=swh_commit_count,output_path="analysis").analyze(workflow)
