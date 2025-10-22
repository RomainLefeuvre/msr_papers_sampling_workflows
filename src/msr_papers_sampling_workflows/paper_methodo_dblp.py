from pathlib import Path

from sampling_mining_workflows_dsl.analysis.CoverageTest import CoverageTest
from sampling_mining_workflows_dsl.analysis.HistWorkflowAnalysis import (
    HistWorkflowAnalysis,
)
from sampling_mining_workflows_dsl.analysis.ChiSquareAnalysis import ChiSquareAnalysis
from sampling_mining_workflows_dsl.element.loader.CsvLoader import CsvLoader
from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata
from sampling_mining_workflows_dsl.toolbox import setup_logging
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder


def main():

    setup_logging(level="INFO",log_file="log.txt")

    # Define the input path and metadata of DBLB dataset
    input_path = Path("data/DBLP/msr.csv")
    title = Metadata.of_string("title")
    year = Metadata.of_integer("year")
    numPages = Metadata.of_integer("numPages")
    title = Metadata.of_string("title")
    doi = Metadata.of_string("DOI")

    # Define the IEEE dataset path and metadata
    IEEE_path = Path("data/IEEE_DATA")
    iee_keyword_list = Metadata.of_list("IEEE Terms", list[str],lambda list_str: list_str.split(";"))

    # Workflow Declaration and Execution
    workflow = (
        WorkflowBuilder()
        .input(CsvLoader(input_path, doi, title, year, numPages))
        .filter_operator("2021 <= year <= 2025")
        .filter_operator("numPages > 6")
        .add_metadata(CsvLoader(IEEE_path, doi, iee_keyword_list))
        .random_selection_operator(cardinality=65, seed=42)
        .output(CsvWriter("data/sample.csv"))
    ).execute_workflow()
     
    # Workflow Execution
#     workflow.execute_workflow()
#     # Workflow Analysis
#    # HistWorkflowAnalysis(year,100,category=True,sort=False).analyze(workflow)
#     HistWorkflowAnalysis(iee_keyword_list,top_x=50,category=True,sort=True).analyze(workflow)

#     set_1 = workflow.get_operator_by_position(2).get_input()
#     set_2 = workflow.get_operator_by_position(2).get_output()

#     ChiSquareAnalysis(iee_keyword_list).analyze(set_1, set_2)
    print(workflow)
    
    # CoverageTest(iee_keyword_list,set_1,set_2)\
    #         .compute_coverage(50)

 


if __name__ == "__main__":
    main()
    # chi2analysis = ChiSquareAnalysis(iee_keyword_list)
    #  set_1 = workflow.get_operator_by_position(2).get_input()
    #  set_2 = workflow.get_operator_by_position(2).get_output()
      #chi2analysis.analyze(set_1, set_2)
    # WorkflowVisualizer(workflow).generate_graph()
    # print(workflow)
    # DistributionWorkflowAnalysis(year).analyze(workflow)