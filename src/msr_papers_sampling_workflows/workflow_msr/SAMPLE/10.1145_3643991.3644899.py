from sampling_mining_workflows_dsl.metadata.Metadata import Metadata
from sampling_mining_workflows_dsl.element.loader.CsvLoader import CsvLoader
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder
from sampling_mining_workflows_dsl.element.Loader import Loader





# 1. start on ecoecosyste.ms7 data (url, list release,latest release)
# 2. add metadata from github (language)
# 3. filter out repo not on github
# 5. filter  repo not with latest action_release between November 2019 and June 2023
# 6, add metadata "dependencies_extractable"
# 7 filter out not dependencicies_extractble

from sampling_workflow.WorkflowBuilder import WorkflowBuilder
from sampling_workflow.element.Loader import Loader
from sampling_workflow.element.CsvWriter import CsvWriter
from sampling_workflow.Metadata import Metadata

# ----------------------------------------------
# Metadata declarations (always before the loader)
# ----------------------------------------------
url = Metadata.of_string("url")
releases = Metadata.of_list("releases")                  # from ecosyste.ms7
latest_release_date = Metadata.of_date("latest_release_date")      # from ecosyste.ms7
language = Metadata.of_string("language")                # from GitHub
dependencies_extractable = Metadata.of_boolean("dependencies_extractable")

# ----------------------------------------------
# Workflow
# ----------------------------------------------
workflow = (
    WorkflowBuilder()
    # 1. start on ecosyste.ms7 dataset
    .input(Loader("ecosyste.ms7", url, releases, latest_release_date))

    # 2. add metadata from GitHub (language)
    .add_metadata(language)

    # 3. filter out repositories not on GitHub
    .filter_operator("language is not '' ")

    # 4. filter repositories with latest release between Nov 2019 and June 2023
    .filter_operator("latest_release_date >= date(2019, 11, 1) and latest_release_date <= date(2023, 6, 30)")

    # 5. add metadata dependencies_extractable
    .add_metadata(Loader(url,dependencies_extractable))

    # 6. filter out repositories without extractable dependencies
    .filter_operator("dependencies_extractable")

    # Export result
    .output(CsvWriter("sample.csv"))
)

workflow.execute_workflow()