from pathlib import Path

# from sampling_workflow.analysis.ChiSquareAnalysis import ChiSquareAnalysis
# from sampling_workflow.analysis.DistributionWorkflowAnalysis import (
#     DistributionWorkflowAnalysis,
# )
# from sampling_workflow.analysis.HistWorkflowAnalysis import HistWorkflowAnalysis
from sampling_workflow.metadata.Metadata import Metadata

from sampling_workflow.element.loader.CsvLoader import CsvLoader
from sampling_workflow.element.writer.CsvWriter import CsvWriter
# from sampling_workflow.exec_visualizer.WorkflowVisualizer import WorkflowVisualizer
from sampling_workflow.WorkflowBuilder import WorkflowBuilder
from sampling_workflow.toolbox import setup_logging

def main():

    setup_logging(level="INFO",log_file="log.txt")

    # Define the input path and metadata of DBLB dataset
    input_path = Path("paper_extension/data/DBLP/msr.csv")
    title = Metadata.of_string("title")
    year = Metadata.of_integer("year")
    numPages = Metadata.of_integer("numPages")
    title = Metadata.of_string("title")
    doi = Metadata.of_string("DOI")

    # Define the IEEE dataset path and metadata
    IEEE_path = Path("paper_extension/methodo/IEEE_DATA")
    iee_keyword_list = Metadata.of_list("IEEE Terms", list[str],lambda list_str: list_str.split(";"))

    # Workflow Declaration and Execution
    workflow = (
        WorkflowBuilder()
        .input(CsvLoader(input_path, doi, title, year, numPages))
        .filter_operator("2021 <= year <= 2025")
        .filter_operator("numPages > 6")
        .add_metadata(CsvLoader(IEEE_path, doi, iee_keyword_list))
        .random_selection_operator(cardinality=65, seed=42)
        .output(CsvWriter("paper_extension/methodo/DBLP/study.csv"))
    )
    # Workflow Execution
    workflow.execute_workflow()
    # Workflow Analysis
    # HistWorkflowAnalysis(year,100,category=True,sort=False).analyze(workflow)
    # HistWorkflowAnalysis(iee_keyword_list,top_x=50,category=True,sort=True).analyze(workflow)
    # CoverageTest(iee_keyword_list,workflow.get_operator_by_position(1).get_output(),
    #                               workflow.get_workflow_output()).compute_coverage(50)

 


if __name__ == "__main__":
    main()
   # chi2analysis = ChiSquareAnalysis(iee_keyword_list)
    # set_1 = workflow.get_operator_by_position(2).get_input()
    # set_2 = workflow.get_operator_by_position(2).get_output()
    # chi2analysis.analyze(set_1, set_2)
    # WorkflowVisualizer(workflow).generate_graph()
    # print(workflow)
    # DistributionWorkflowAnalysis(year).analyze(workflow)