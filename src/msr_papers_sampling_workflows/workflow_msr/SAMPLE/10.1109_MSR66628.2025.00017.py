# 1.start from github 
# 2. add metadata " contains_targeted_commit", (those hat contain at least one commit matching (remove|delete).*?console.log.)
# 2.filter repository contains_targeted_commit

from pathlib import Path

from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.element.loader import Loader
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder
from sampling_mining_workflows_dsl.operator.OperatorBuilder import OperatorBuilder
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata

def main():
    url = Metadata.of_string("url")

    contains_targed_commits = Metadata.of_boolean("contains_targeted_commits")

   
    WorkflowBuilder()\
            .input(Loader("github_loader", url))\
            .add_metadata_filter_operator(contains_targed_commits)\
            .filter_operator(contains_targed_commits.is_true())
    