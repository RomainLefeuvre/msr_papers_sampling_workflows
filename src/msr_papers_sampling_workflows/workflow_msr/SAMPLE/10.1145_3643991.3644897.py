
from sampling_mining_workflows_dsl.element.writer.CsvWriter import CsvWriter
from sampling_mining_workflows_dsl.element.loader.CsvLoader import CsvLoader
from sampling_mining_workflows_dsl.element.Loader import Loader
from sampling_mining_workflows_dsl.metadata.Metadata import Metadata
from sampling_mining_workflows_dsl.WorkflowBuilder import WorkflowBuilder

# For y corpus : 
# * Start from existing dataset
# * add test_coverage metadata
# * Filter for repos with > 80% test coverage
# * add is_buildable metadadata
# * add construct_dynamic_cg 
# * add construct_static_cg
# * filter is_buildable and construct_dynamic_cg and construct_static_cg
# *export YCorpus.csv

# For X corpus :
# * add is_buildable metadadata
# * add construct_dynamic_cg 
# * add construct_static_cg
# * filter is_buildable and construct_dynamic_cg and construct_static_cg
# * export XCorpus.csv

# For final 
# * Start from Y, union X, union NJR
# * export NYXCorpus.csv


def main():
    # --- Metadata declarations ---
    repo_id = Metadata.of_string("repo_id")
    test_coverage = Metadata.of_double("test_coverage")
    is_buildable = Metadata.of_boolean("is_buildable")
    construct_dynamic_cg = Metadata.of_boolean("construct_dynamic_cg")
    construct_static_cg = Metadata.of_boolean("construct_static_cg")

    # --- Y Corpus Workflow ---
    y_corpus_workflow = (
        WorkflowBuilder()
        # Start from existing dataset
        .input(
            CsvLoader(
                "Khatami_Zaidman_dataset.csv",
                repo_id,
            )
        )
        # Add test_coverage metadata
        .add_metadata(Loader(test_coverage))
        # Filter for repos with > 80% test coverage
        .filter_operator("test_coverage > 0.80")
        # Add is_buildable metadata
        .add_metadata(Loader(is_buildable))
        # Add construct_dynamic_cg metadata
        .add_metadata(Loader(construct_dynamic_cg))
        # Add construct_static_cg metadata
        .add_metadata(Loader(construct_static_cg))
        # Filter is_buildable and construct_dynamic_cg and construct_static_cg
        .filter_operator(is_buildable.is_true())
        .filter_operator(construct_dynamic_cg.is_true())
        .filter_operator(construct_static_cg.is_true())
        # Export YCorpus.csv
        .output(CsvWriter("YCorpus.csv"))
    )

    # --- X Corpus Workflow ---
    x_corpus_workflow = (
        WorkflowBuilder()
        # Start from X dataset
        .input(
            CsvLoader("XCorpus_original.csv" ,
                repo_id,
            )
        )
        # Add is_buildable metadata
        .add_metadata(Loader(is_buildable))
        # Add construct_dynamic_cg metadata
        .add_metadata(Loader(construct_dynamic_cg))
        # Add construct_static_cg metadata
        .add_metadata(Loader(construct_static_cg))
        # Filter is_buildable and construct_dynamic_cg and construct_static_cg
        .filter_operator(is_buildable.is_true())
        .filter_operator(construct_dynamic_cg.is_true())
        .filter_operator(construct_static_cg.is_true())
        # Export XCorpus.csv
        .output(CsvWriter("XCorpus.csv"))
    )

    # --- Final NYX Corpus Workflow ---
    final_corpus_workflow = (
        WorkflowBuilder()
        # Start from Y corpus
        .input(
            CsvLoader(
                "YCorpus.csv",
                repo_id,
            )
        )
        # Union with X corpus
        .union_with_external_set_operator(
            CsvLoader(
                "XCorpus.csv",
                repo_id,
               
            )
        )
        # Union with NJR corpus
        .union_with_external_set_operator(
            CsvLoader(
                "NJRCorpus_original.csv",
                repo_id,
            )
        )
        # Export NYXCorpus.csv
        .output(CsvWriter("NYXCorpus.csv"))
    )

    # Execute workflows
    print("Executing Y Corpus workflow...")
    y_corpus_workflow.execute_workflow()
    
    print("Executing X Corpus workflow...")
    x_corpus_workflow.execute_workflow()
    
    print("Executing Final NYX Corpus workflow...")
    final_corpus_workflow.execute_workflow()


if __name__ == "__main__":
    main()
