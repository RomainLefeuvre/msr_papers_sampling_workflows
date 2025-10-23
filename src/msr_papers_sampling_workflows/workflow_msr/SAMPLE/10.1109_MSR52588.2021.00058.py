
from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.element.Loader import Loader
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

# Characterising the Knowledge about Primitive Variables in Java Code Comments.


# * Start from github
# * filter java project
# * filter project with at least one star
# * add metadata available_on_github
# * filter repo available_on_github
# * add engineered_project metadata with repo reaper
# * filter engineered_project
# * add metadata  have_readme
# * filter project having readme
# * add metadata documentation_in_readme
# * filter documentation_in_readme
# * filter out project without java file

# ---- Metadata ----
url = Metadata.of_string("id")
language = Metadata.of_string("language")
stars = Metadata.of_integer("stars")
available_on_github = Metadata.of_boolean("available_on_github")
engineered_project = Metadata.of_boolean("engineered_project")
have_readme = Metadata.of_boolean("have_readme")
documentation_in_readme = Metadata.of_boolean("documentation_in_readme")
contain_java_file = Metadata.of_boolean("containJavaFile")


def main():
    workflow = (
        WorkflowBuilder()
        # Start from GitHub dataset
        .input(
            Loader(
                url,
                language,
                stars,
                available_on_github,
                engineered_project,
                have_readme,
                documentation_in_readme,
                contain_java_file,
            )
        )
        # Filter Java projects
        .add_metadata(Loader(url, language))
        .filter_operator("language == 'Java'")
        # Filter project with at least one star
        .add_metadata(Loader(url, stars))
        .filter_operator("stars > 0")
        # Add & filter: available on GitHub
        .add_metadata(Loader(url, available_on_github))
        .filter_operator("available_on_github")
        # Add & filter: engineered project (via RepoReaper)
        .add_metadata(Loader(url, engineered_project))
        .filter_operator("engineered_project")
        # Add & filter: must have a README
        .add_metadata(Loader(url, have_readme))
        .filter_operator("have_readme")
        # Add & filter: documentation must be in README
        .add_metadata(Loader(url, documentation_in_readme))
        .filter_operator("documentation_in_readme")
        # filter out project without java file
        .add_metadata(contain_java_file)
        .filter_operator("contain_java_file")
        # Final output
        .output(CsvWriter("github_java_engineered.csv"))
    )

    workflow.execute_workflow()
