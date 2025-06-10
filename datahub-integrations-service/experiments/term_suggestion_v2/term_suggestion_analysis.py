import glob
import os
import pathlib
import pickle
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

import dotenv
import json5
import nest_asyncio
from term_suggestion_analysis_helper import (
    convert_parsed_response_to_readable_csv,
    get_average_run_scores_df,
    get_combined_data,
    get_parsed_responses_for_single_experiment_run,
    get_table_and_column_infos_dict,
    populate_analysis_for_confidence_threshold_list,
    write_llm_output_to_csv,
)
from tqdm import tqdm

from datahub_integrations.gen_ai.term_suggestion_v2 import (
    _USE_EXTRACTION,
    COLUMN_SPLIT_LENGTH,
    NUM_GLOSSARY_TERMS_IN_BATCH,
    TEMPERATURE,
)
from datahub_integrations.gen_ai.term_suggestion_v2_context import (
    GlossaryInfo,
    GlossaryUniverseConfig,
    fetch_glossary_info,
)

current_dir = pathlib.Path().parent.resolve()
sys.path.append(str(current_dir.parent))
from docs_generation.graph_helper import create_datahub_graph

# Apply the patch to allow nested event loops
nest_asyncio.apply()
dotenv.load_dotenv(current_dir / "resource_files/.env")


@dataclass
class ExperimentConfig:
    prompt_path: str
    experiment_name: str
    iterations: int
    confidence_thresholds: List[float]


experiment_dict = [
    ExperimentConfig(
        prompt_path="prompts/elaborated_confidence_prompt.txt",
        experiment_name="elaborated_confidence_prompt",
        iterations=2,
        confidence_thresholds=[6, 7, 8, 9, 10],
    ),
]


# Experiment settings:
print("NUM_GLOSSARY_TERMS_IN_BATCH: ", NUM_GLOSSARY_TERMS_IN_BATCH)
print("COLUMN_SPLIT_LENGTH: ", COLUMN_SPLIT_LENGTH)
print("_USE_EXTRACTION: ", _USE_EXTRACTION)
print("TEMPERATURE: ", TEMPERATURE)

BASE_DIRECTORY = (
    current_dir / f"exp_output/output_{datetime.now().strftime('%m-%d-%H-%M')}/"
)
print("BASE_DIRECTORY: ", BASE_DIRECTORY)
RESOURCE_DIR = current_dir / "resource_files/"
CONFIDENCE_THRESHOLD = 9.0

COLUMN_LABELS_CSV_PATH = RESOURCE_DIR / "cisco_ground_truth_19_12.csv"
LABEL_COLUMN = "ground_truth_new"

GLOSSARY_TERMS_INFO_FILE_PATH = None  # Specify
GLOSSARY_INSTANCE = "cisco"
GLOSSARY_NODES = [
    "urn:li:glossaryNode:f401dc4237252fd84aef62f197c6ff6d",
]
GLOSSARY_TERMS = []
USE_TERM_PARENT_INFO = True

INSTANCES_TO_EXAMINE = ["cisco"]

URNS_DICT_PATH = RESOURCE_DIR / "test_urns.json"
URNS_DICT: Dict[str, List[str]] = json5.loads(  # type: ignore
    URNS_DICT_PATH.read_text()
)

FILTERS = [
    "no_filter",
    # "description",
    # "sample_values",
    # "description_and_sample_values",
    # "description_or_sample_values",
    # "name_and_datatype",
]


def save_experiment_details(glossary):
    with open(BASE_DIRECTORY / "experiment_details.txt", "w") as f:
        f.write(
            f"NUM_GLOSSARY_TERMS_IN_BATCH:-  {NUM_GLOSSARY_TERMS_IN_BATCH}\n"
            f"COLUMN_SPLIT_LENGTH:- {COLUMN_SPLIT_LENGTH}\n"
            f"_USE_EXTRACTION:- {_USE_EXTRACTION}\n"
            f"USE_TERM_PARENT_INFO:- {USE_TERM_PARENT_INFO}\n"
            f"COLUMN_LABELS_CSV_PATH:- {COLUMN_LABELS_CSV_PATH}\n"
            f"GLOSSARY_TERMS_INFO_FILE_PATH:- {GLOSSARY_TERMS_INFO_FILE_PATH}\n"
            f"INSTANCES_TO_EXAMINE:- {INSTANCES_TO_EXAMINE}\n"
            f"GLOSSARY_INSTANCE:- {GLOSSARY_INSTANCE}\n"
            f"GLOSSARY_NODES:- {GLOSSARY_NODES}\n"
            f"GLOSSARY_TERMS:- {GLOSSARY_TERMS}\n"
            f"LABEL_COLUMN:- {LABEL_COLUMN}\n"
            f"URNS_DICT_PATH:- {URNS_DICT_PATH}\n"
            f"FILTERS:- {FILTERS}\n"
            f"TEMPERATURE:- {TEMPERATURE}"
            f"THRESHOLD:- {CONFIDENCE_THRESHOLD}"
        )
        f.close()
    with open(BASE_DIRECTORY / "glossary.json", "w") as file_:
        json5.dump(glossary, file_)
    return None


URNS_DICT = {
    key: value for key, value in URNS_DICT.items() if key in INSTANCES_TO_EXAMINE
}

# Get table and column info:
tables_info_dict, columns_info_dict = get_table_and_column_infos_dict(
    urns_dict=URNS_DICT
)

# Fetch glossary info\
if GLOSSARY_TERMS_INFO_FILE_PATH is not None:
    with open(GLOSSARY_TERMS_INFO_FILE_PATH, "r") as f:
        glossary_terms_info = json5.load(f)
    glossary_info = GlossaryInfo(glossary=glossary_terms_info)
else:
    graph_client = create_datahub_graph(GLOSSARY_INSTANCE)
    glossary_config = GlossaryUniverseConfig(
        glossary_nodes=GLOSSARY_NODES, glossary_terms=GLOSSARY_TERMS
    )
    glossary_info = fetch_glossary_info(
        graph_client=graph_client, universe=glossary_config
    )

# Remove duplicate terms
old_glossary = glossary_info.glossary
used_names = []
new_glossary = {}
print("Duplicate Terms....")
for key, value in old_glossary.items():
    if value["term_name"] not in used_names:
        new_glossary[key] = value
        used_names.append(value["term_name"])
    else:
        print(value["term_name"])
        continue
glossary_info.glossary = new_glossary

if not USE_TERM_PARENT_INFO:
    print("removing parent node info ......")
    for key, _value in glossary_info.glossary.items():
        glossary_info.glossary[key].pop("parent_node", None)

terms = []
for _key, value in glossary_info.glossary.items():
    terms.append(value["term_name"])
print("Number of terms available: ", len(terms))
print("Available Terms: ", terms)


for experiment in tqdm(experiment_dict, total=len(experiment_dict)):
    (BASE_DIRECTORY / f"{experiment.experiment_name}").mkdir(
        parents=True, exist_ok=True
    )
    # Save Experiment Details
    save_experiment_details(glossary_info.glossary)

    for run in range(experiment.iterations):
        print(f"run_{run}")
        # Create Output Directory:
        OUTPUT_DIR = (
            BASE_DIRECTORY
            / f"{experiment.experiment_name}"
            / f"run{run + 1}_{datetime.now().strftime('%H-%M-%S')}"
        )
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        # read and save prompt:
        prompt_path = current_dir / experiment.prompt_path
        prompt = prompt_path.read_text()
        pathlib.Path(OUTPUT_DIR / "prompt.txt").write_text(prompt)

        # Generate Response:
        parsed_llm_responses, raw_llm_responses = (
            get_parsed_responses_for_single_experiment_run(
                urns_dict=URNS_DICT,
                glossary_info=glossary_info,
                prompt_path=prompt_path,
            )
        )
        # Write response to directory
        write_llm_output_to_csv(
            llm_response=parsed_llm_responses,
            csv_path=OUTPUT_DIR / "parsed_response.csv",
        )
        try:
            convert_parsed_response_to_readable_csv(
                parsed_responses=parsed_llm_responses,
                columns_info_dict=columns_info_dict,
                destination_csv_path=OUTPUT_DIR
                / "parsed_response_with_high_confidence.csv",
                threshold=CONFIDENCE_THRESHOLD,
            )
        except Exception as e:
            print(
                f"Could not convert parsed response to readable csv due to exception {e}!!"
            )
        with open(OUTPUT_DIR / "parsed_response.pkl", "wb") as fp:
            pickle.dump(parsed_llm_responses, fp)

        print("===============================\n\n")

        # Populate Analysis:
        populate_analysis_for_confidence_threshold_list(
            output_dir=OUTPUT_DIR,
            confidence_thresholds=experiment.confidence_thresholds,
            filters=FILTERS,
            urns_dict=URNS_DICT,
            parsed_llm_responses=parsed_llm_responses,
            columns_info_dict=columns_info_dict,
            column_labels_csv_path=COLUMN_LABELS_CSV_PATH,
            label_column=LABEL_COLUMN,
        )

    input_dir = BASE_DIRECTORY / f"{experiment.experiment_name}"
    runs_list = [run_name for run_name in os.listdir(input_dir) if "run" in run_name]
    run_paths = glob.glob(f"{input_dir}/run*")
    predictions_file_name = "parsed_response_with_high_confidence.csv"
    common_columns = ["urn", "instance", "column", "datatype"]
    scores_file_name = "final_stats_threshold_9.csv"

    score_df = get_average_run_scores_df(
        run_paths=run_paths, scores_file_name=scores_file_name
    )
    combined_predictions_df, value_counts_df = get_combined_data(
        run_paths=run_paths,
        predictions_file_name=predictions_file_name,
        common_columns=common_columns,
        num_iter=experiment.iterations,
        threshold=CONFIDENCE_THRESHOLD,
    )
    combined_predictions_df.to_csv(
        input_dir / "_combined_run_predictions.csv", index=False
    )
    value_counts_df.to_csv(input_dir / "_value_counts.csv")
    score_df.to_csv(input_dir / "_aggregated_scores.csv")

print("All Experiments Completed!!!")
