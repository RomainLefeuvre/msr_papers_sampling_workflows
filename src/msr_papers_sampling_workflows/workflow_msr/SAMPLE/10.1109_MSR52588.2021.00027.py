from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.element.Loader import Loader
from sampling_mining_workflows_dsl.element.loader.LoaderFactory import *
from sampling_mining_workflows_dsl.element.writer.WriterFactory import *
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

# An Exploratory Study of Log Placement
# Recommendation in an Enterprise System

# start from apache foundation list
# filter java project
# add is_clonable metadata
# filter clonable project
# add percentage_log_method
# filter project with at least 4% of percentage_log_method

url = Metadata.of_string("id")
language = Metadata.of_string("language")
is_clonable = Metadata.of_boolean("isClonable")
percentage_log_method = Metadata.of_double("percentageLogMethod")


def apache_foundation_workflow():
    return (
        WorkflowBuilder()
        # Start from Apache Foundation list
        .input(
            Loader(
                url,
                language,
                is_clonable,
                percentage_log_method,
            )
        )
        # Filter Java projects
        .filter_operator("language == 'Java'")
        # Add clonable metadata
        .add_metadata(is_clonable)
        # Filter clonable projects
        .filter_operator("isClonable == True")
        # Add percentage_log_method metadata
        .add_metadata(percentage_log_method)
        # Filter project with at least 4% logging methods
        .filter_operator("percentageLogMethod >= 0.04")
        .output(CsvWriter("apache_foundation_filtered.csv"))
    )


def main():
    workflow = apache_foundation_workflow()
    workflow.execute_workflow()
