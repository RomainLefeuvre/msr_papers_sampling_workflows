from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata

from sampling_mining_workflows_dsl.element.Loader import Loader
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

# ---- Metadata ----
url = Metadata.of_string("id")  # repository id
rate_non_contributor_issue = Metadata.of_double("rate_non_contributor_issue")
first_commit_date = Metadata.of_long("first_commit_date")
latest_issue_date = Metadata.of_long("latest_issue_date")


def main():
    workflow = (
        WorkflowBuilder()
        .input(
            Loader(
                url, rate_non_contributor_issue, first_commit_date, latest_issue_date
            )
        )
        # Filter by rate of issues opened by non-contributors
        .add_metadata(Loader(url, rate_non_contributor_issue))
        .filter_operator("rate_non_contributor_issue > X")
        # Filter repositories with first commit between 2008 and 2014
        .add_metadata(Loader(url, first_commit_date))
        .filter_operator(
            "first_commit_date >= date(2008, 1, 1) and first_commit_date <= date(2014, 12, 31)"
        )
        # Filter repositories with latest issue after Jan 1, 2019
        .add_metadata(Loader(latest_issue_date))
        .filter_operator("latest_issue_date > date(2019, 1, 1)")
        # Output filtered dataset
        .output(CsvWriter("github_non_contrib_issues.csv"))
    )

    workflow.execute_workflow()
