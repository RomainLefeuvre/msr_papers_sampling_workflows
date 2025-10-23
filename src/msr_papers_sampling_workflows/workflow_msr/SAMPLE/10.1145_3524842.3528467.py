

from sampling_mining_workflows_dsl.element.Loader import Loader
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

url = Metadata.of_string("url")
language = Metadata.of_string("language")
creation_date = Metadata.of_long("creation_date")


def dataset1():
    return (
        WorkflowBuilder()
        .input(Loader(url, language, creation_date))
        # Filter Python projects
        .add_metadata(Loader(url, language))
        .filter_operator("language == 'Python'")
        # Filter projects created after March 20, 2019
        .add_metadata(Loader(url, creation_date))
        .filter_operator("creation_date > date(2019, 3, 20)")
    )


def dataset2():
    return (
        WorkflowBuilder()
        .input(Loader(url, language, creation_date))
        # Filter Python projects
        .add_metadata(Loader(url, language))
        .filter_operator("language == 'Python'")
        # Filter projects created after March 11, 2021
        .add_metadata(Loader(url, creation_date))
        .filter_operator("creation_date > date(2021, 3, 11)")
    )
