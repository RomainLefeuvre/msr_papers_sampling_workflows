from pathlib import Path

from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.element.Loader import Loader
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

# It Works (only) on My Machine: A Study on Reproducibility Smells in Ansible Scripts.

# 1. Sort repos by number of downloads
# 2. Filter repos with more than 10 scripts
# 3. Filter repos with 1 000 stars
# 4. Filter repos with 300 commits
# 5. Manual check if contains Ansible scripts
# 6. Include oci-ansible-collection dataset


def main():
    input_path = Path("ansible-galaxy.json")
    downloads_nb = Metadata.of_integer("downloads")
    scripts_nb = Metadata.of_integer("scripts_nb")
    stars_nb = Metadata.of_integer("stars")
    commits_nb = Metadata.of_integer("commits")

    url = Metadata.of_string("url")

    cardinality = 42  # Ambiguous

    # Workflow Declaration and Execution
    workflow = (
        WorkflowBuilder()
        .input(
            Loader(input_path, url, scripts_nb, stars_nb, commits_nb)
        )
        .add_metadata(downloads_nb)
        .systematic_selection_operator(
            cardinality, downloads_nb, 1
        )  # Sort by downloads
        .filter_operator("scripts_nb > 10")  # Filter repos with more than 10 scripts
        .filter_operator("stars = 1000")  # Filter repos with more than 1000 stars
        .filter_operator("commits = 300")  # Filter repos with more than 300 commits
        .manual_sampling_operator()  # Manual check if contains Ansible scripts
        # Include oci-ansible-collection dataset
        .output(CsvWriter("out.csv"))
    )

    workflow.execute_workflow()
    # 1. start from github
    # 2. Sort repos by number of downloads
    # 3. select top 20
    # 4. filter repo with at least 100 commits 
    # 5. select randomly 8 repo
    # 6. Manual sampling 
    

    workflow = (
        WorkflowBuilder()
        .input(
            Loader("Github", url, commits_nb)
        )
        .add_metadata(downloads_nb)
        .systematic_selection_operator(
            20, downloads_nb, 1
        )  # Sort by downloads
        .filter_operator("commits = 100") 
        .manual_sampling_operator()  
        
        .output(CsvWriter("out2.csv"))
    )

    workflow2
if __name__ == "__main__":
    main()
