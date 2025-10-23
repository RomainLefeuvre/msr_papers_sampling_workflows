from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.element.Loader import Loader
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

# PeaTMOSS: A Dataset and Initial Analysis of Pre-Trained Models in Open-Source Software.
# * Filter for public repos
# * Filter for non-forked
# * Filter for non-archived
# * Filter for >= 5 stars
# * Check for signature of PTM usage in apps 

def main():
    # --- Metadata declarations ---
    url = Metadata.of_string("id")
    is_public = Metadata.of_boolean("isPublic")
    is_fork = Metadata.of_boolean("isFork")
    is_archived = Metadata.of_boolean("isArchived")
    stars = Metadata.of_long("stars")
    use_PTM = Metadata.of_boolean("usePTM")


    # --- Workflow ---
    (
        WorkflowBuilder()
        .input(
            Loader(
                url,
                is_public,
                is_fork,
                is_archived,
                stars,
                use_PTM,
            )
        )
        .filter_operator("is_public")
        .filter_operator("not is_fork")
        .filter_operator("not is_archived")
        .filter_operator("stars >= 5")
        .filter_operator("use_PTM")
        
    )