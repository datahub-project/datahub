# Experiment Automation and Data Processing Documentation

## Overview

The Python script `term_suggestion_analysis.py` automates the process of running multiple experiments based on defined configurations. The script performs several key tasks including generating LLM (Language Model) responses, analyzing those responses and saving outputs for further analysis.

The script outputs a variety of files including parsed responses, aggregated scores and predictions.

## Key Modules

Various helper and integration modules are imported:

- **datahub_integrations.gen_ai**: Used for fetching and processing glossary data, table & column information and getting term recommendations from LLM.
- **docs_generation.graph_helper**: Used for creating DataHub graphs.
- **term_suggestion_analysis_helper**: Contains functions for analyzing and aggregating experiment results.

## Configuration and Constants

### Experiment Configuration

The configuration for experiments is stored in the `experiment_dict` list, which contains `ExperimentConfig` instances. Each experiment can define:

- `prompt_path`: Path to the prompt file used to generate responses.
- `experiment_name`: Name of the experiment.
- `iterations`: Number of times the experiment should run.
- `confidence_thresholds`: A list of thresholds for confidence, used in response analysis.

Example:

```python
ExperimentConfig(
    prompt_path="prompts/elaborated_confidence_prompt.txt",
    experiment_name="elaborated_confidence_prompt",
    iterations=10,
    confidence_thresholds=[6, 7, 8, 9, 10],
)
```

### Required File Paths

- `graph_credentials.json` - This file should be present under `datahub-integrations-service\experiments\docs_generation` directory. It contains tokens for all instances
- `resource_files` - This directory needs to be present in `datahub-integrations-service\experiments\term_suggestion_v2`, it contains following files
  - `test_urns.json` - This file contains table urns for all instances
  - `.env` - Specify values of environment variables BEDROCK_AWS_ACCESS_KEY_ID and BEDROCK_AWS_SECRET_ACCESS_KEY
  - Ground truth CSV (e.g. `cisco_ground_truth_19_12.csv`, `column_labels_without_care_data_v5.csv`, `care_columns_ground_truth_v3.csv`)

### Constants for Experiment Behavior

- `NUM_GLOSSARY_TERMS_IN_BATCH`, `COLUMN_SPLIT_LENGTH`, `_USE_EXTRACTION`, `TEMPERATURE`: Constants that control the behavior of the experiment generation, such as batch sizes, extraction flags, and temperature settings for response generation.
- `CONFIDENCE_THRESHOLD`: The confidence threshold used for filtering and analyzing responses.
- `COLUMN_LABELS_CSV_PATH`: Ground truth file path
- `LABEL_COLUMN`: Ground truth column name
- `GLOSSARY_TERMS_INFO_FILE_PATH`: JSON file path to load the glossary terms, if `None` then glossary terms are fetched from specified `GLOSSARY_INSTANCE`
- `GLOSSARY_INSTANCE`, `GLOSSARY_NODES`, `GLOSSARY_TERMS`: Glossary nodes and terms to be fetched from glossary instance. If `GLOSSARY_TERMS_INFO_FILE_PATH` is None then only glossary terms are fetched.
- `USE_TERM_PARENT_INFO`: Switch to use glossary term parent information
- `INSTANCES_TO_EXAMINE`: A list specifying instance to examine

## Functionality Overview

### Directory Setup and File Handling

- The script sets up directories for storing outputs from each experiment iteration using `pathlib`. The directories are created dynamically for each run, with timestamps to ensure unique names.
- It saves experiment details to a text file and outputs the glossary in a JSON format for reproducibility.

### Fetching and Cleaning Glossary Data

- Glossary data is fetched either from a local file (`GLOSSARY_TERMS_INFO_FILE_PATH`) or from an external DataHub service using a client.
- The glossary is processed to remove duplicate terms and filter out unnecessary parent node information if specified.

### Running Experiments

For each experiment in `experiment_dict`, the script:

1. Sets up the experiment directory and saves the experiment details.
2. Iterates through each run (as specified in `iterations`).
   - Creates an output directory for each run.
   - Loads the prompt from the specified file and writes it to the output directory.
   - Generates LLM responses based on the prompt, processes them, and saves them to CSV files.
   - Analyzes the responses based on confidence thresholds, filters the responses, and aggregates statistics.

### Data Processing and Analysis

- The experiment results are aggregated into dataframes using functions from `term_suggestion_analysis_helper`. These functions:
  - `get_table_and_column_infos_dict`: Retrieves table and column information from the provided URN mappings.
  - `get_parsed_responses_for_single_experiment_run`: Generates glossary recommedations and parses the raw LLM responses.
  - `populate_analysis_for_confidence_threshold_list`: Analyzes the parsed responses based on the defined confidence thresholds.
  - `get_combined_data`: Combines predictions across all runs and produces aggregated statistics.
  - `get_average_run_scores_df`: Aggregates scores from different runs to calculate averages.

### Output Files

- The script saves output in the `BASE_DIRECTORY`. It creates a experiment specific directory in `BASE_DIRECTORY` and generates following output
  - run specific directory containing
    - performance numbers with different confidence threshold value
    - `prompt.txt`: The prompt used to generate LLM responses.
    - `parsed_response.csv`: Parsed LLM responses saved in CSV format.
    - `parsed_response_with_high_confidence.csv`: Filtered responses above a specified confidence threshold.
  - Aggregated scores and value counts are saved as CSV files
    - `_combined_run_predictions.csv`: predictions across different runs with confidence >= confidence thresholds
    - `_value_counts.csv` - counts indicating consistency behavior
    - `_aggregated_scores.csv` - averaged performance numbers across all runs

### Final Results

Once all iterations of all experiments are completed, the script outputs a message indicating the successful completion of all experiments.

## Conclusion

This script is designed to streamline the process of running multiple experiments, generating LLM responses, and performing detailed analyses on the results. By automating these tasks, it ensures reproducibility and efficiency, enabling researchers to focus on interpreting the outcomes of the experiments rather than managing the logistics.
