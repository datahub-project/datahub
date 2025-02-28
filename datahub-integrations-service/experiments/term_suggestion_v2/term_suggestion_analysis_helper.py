import ast
import csv
import os
import pathlib
import sys
from datetime import datetime
from functools import reduce
from typing import Dict, List, Optional, Tuple

import json5
import nest_asyncio
import numpy as np
import pandas as pd
from loguru import logger
from pydantic import BaseModel

from datahub_integrations.gen_ai.description_v2 import (
    extract_metadata_for_urn,
    transform_table_info_for_llm,
)
from datahub_integrations.gen_ai.term_suggestion_v2 import (
    TermSuggestionBundle,
    get_term_recommendations,
)

current_dir = pathlib.Path().parent.resolve()
sys.path.append(str(current_dir.parent))
from docs_generation.graph_helper import create_datahub_graph

nest_asyncio.apply()

GROUND_TRUTH_TERM_SEP = "/"


class SerializedResponse(BaseModel):
    response_dict: List[
        Tuple[
            str,
            str,
            Optional[List[TermSuggestionBundle]],
            Optional[Dict[str, List[TermSuggestionBundle]]],
        ]
    ]


def get_table_and_column_infos_dict(
    urns_dict: Dict[str, List[str]]
) -> tuple[dict, dict]:
    table_infos_dict = {}
    column_infos_dict = {}
    for instance, urns in urns_dict.items():
        graph_client = create_datahub_graph(instance)
        for table_urn in urns:
            print(table_urn)
            try:
                entity = graph_client.get_entity_semityped(table_urn)
                extracted_entity_info = extract_metadata_for_urn(
                    entity, table_urn, graph_client
                )
                table_info, column_info = transform_table_info_for_llm(
                    extracted_entity_info
                )
                table_infos_dict[table_urn] = table_info
                column_infos_dict[table_urn] = column_info
            except Exception as e:
                print(e)
                continue
    return table_infos_dict, column_infos_dict


def get_failed_response_table_urns(parsed_llm_responses) -> tuple[list[str], list[str]]:
    tables_with_parsing_error = []
    skipped_tables = []
    for item in parsed_llm_responses:
        urn = item[0]
        column_terms = item[3]
        if column_terms is None:
            tables_with_parsing_error.append(urn)
        elif len(column_terms) == 0:
            skipped_tables.append(urn)
        #             print(len(column_terms))
        else:
            #             print(urn)
            #             print(len(column_terms))
            continue
    return tables_with_parsing_error, skipped_tables


def read_labeled_column_data(csv_path, urns_dict) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.loc[:, "unique_keys"] = df.table_urn + "_" + df.col_name
    df = df.replace([np.nan], [None])
    df = df[
        df["table_urn"].isin([urn for value in urns_dict.values() for urn in value])
    ]
    df = df.reset_index(drop=True)
    return df


def get_prediction_df(parsed_llm_responses, confidence_threshold=8):
    terms_assigned = {}
    for response in parsed_llm_responses:
        #         print(response[0], response[1])
        table_urn = response[0]
        column_terms = response[3]
        if column_terms is not None:
            for column_name, terms in column_terms.items():
                unique_key = table_urn + "_" + column_name
                if terms is not None:
                    terms_list = [
                        (term.name, term.confidence_score, term.reasoning)
                        for term in terms
                        if not term.is_fake
                    ]
                else:
                    terms_list = []
                terms_assigned[unique_key] = terms_list
    df_pred = pd.DataFrame(
        {
            "unique_keys": list(terms_assigned.keys()),
            "predicted_labels": list(terms_assigned.values()),
        }
    )
    df_pred.loc[:, "pred_max_score_term"] = df_pred["predicted_labels"].apply(
        lambda x: [
            term[0]
            for term in x
            if term[1] >= confidence_threshold
            # if (term[1] == max([i[1] for i in x])) and (term[1] >= confidence_threshold)
        ]
    )
    return df_pred


def filter_predictions_df(df: pd.DataFrame, filter_="no_filter") -> pd.DataFrame:
    if filter_ == "description":
        return df[~df.col_description.isnull()]
    elif filter_ == "sample_values":
        return df[~df.sample_values.isnull()]
    elif filter_ == "description_and_sample_values":
        return df[(~df.col_description.isnull()) & (~df.sample_values.isnull())]
    elif filter_ == "name_and_datatype":
        return df[df.col_description.isnull() & df.sample_values.isnull()]
    elif filter_ == "description_or_sample_values":
        return df[(~df.col_description.isnull()) | (~df.sample_values.isnull())]
    else:
        return df


def get_table_fake_columns_data(column_terms, all_column_names):
    fake_columns_count = 0
    if column_terms is not None and not isinstance(column_terms, str):
        for key, values in column_terms.items():
            try:
                if key not in all_column_names:
                    #                     fake_columns.append(key)
                    fake_columns_count += 1

            except Exception as e:
                print(f"Exception!! {e}")
                continue
    return fake_columns_count


def get_table_fake_terms_data(column_terms, columns):
    fake_terms_count = 0
    if column_terms is not None and not isinstance(column_terms, str):
        for key, values in column_terms.items():
            try:
                if key in columns:
                    for value in values:
                        if value.is_fake is not None and value.is_fake:
                            fake_terms_count += 1
            except Exception as e:
                print(f"Exception!! {e}")
                continue
    return fake_terms_count


def get_classification_stats(table_urn, pred_df):
    #     for table_urn in table_wise_analysis_dict.keys():
    df_temp = pred_df[pred_df.table_urn == table_urn]
    label_class_counts = dict(df_temp.label_class.value_counts())
    TP = label_class_counts.get("match-term_assigned", 0)
    TN = label_class_counts.get("match-no_assignment", 0)
    FP1 = label_class_counts.get("mismatch-predicted_only", 0)
    FP2 = label_class_counts.get("mismatch", 0)
    FN = label_class_counts.get("mismatch-actual_only", 0)
    #     print(label_class_counts)
    #     print('TP: ', TP, ' FP1: ', FP1, " FP2: ", FP2, ' TN: ', TN, ' FN: ', FN)
    precision_term = TP / (TP + FP1 + FP2) if TP + FP1 + FP2 != 0 else np.nan
    recall_term = TP / (TP + FN + FP2) if TP + FN + FP2 != 0 else np.nan
    precision_none = TN / (TN + FN) if TN + FN != 0 else np.nan
    recall_none = TN / (TN + FP1) if TN + FP1 != 0 else np.nan
    return {
        "TP": TP,
        "TN": TN,
        "FP1": FP1,
        "FP2": FP2,
        "FN": FN,
        "recall_term": recall_term,
        "precision_term": precision_term,
        "recall_none": recall_none,
        "precision_none": precision_none,
    }


def get_classification_report_df(
    parsed_llm_responses,
    labeled_df,
    pred_df,
    column_infos_dict,
    filter_="no_filter",
):
    table_wise_analysis_dict = {}
    for i, response in enumerate(parsed_llm_responses):
        try:
            column_stats_dict = {}
            table_urn = response[0]
            column_terms = response[3]
            column_terms_character_length = len(str(column_infos_dict[table_urn]))

            if column_terms is not None:

                # all_assigned_column_names = [column for column in column_terms.keys()]
                all_actual_column_names = list(column_infos_dict[table_urn].keys())

                filtered_actual_column_names = list(
                    filter_predictions_df(
                        labeled_df[labeled_df.table_urn == table_urn], filter_
                    ).col_name
                )

                filtered_assigned_column_names = [
                    column
                    for column in column_terms.keys()
                    if column in filtered_actual_column_names
                ]

                fake_columns_count = get_table_fake_columns_data(
                    column_terms, all_actual_column_names
                )
                column_stats_dict["fake_columns_count"] = fake_columns_count

                fake_terms_count = get_table_fake_terms_data(
                    column_terms, filtered_actual_column_names
                )
                column_stats_dict["fake_terms_count"] = fake_terms_count

                column_stats_dict["actual_column_count"] = len(
                    filtered_actual_column_names
                )
                column_stats_dict["skipped_columns_count"] = len(
                    filtered_actual_column_names
                ) - len(filtered_assigned_column_names)
                column_stats_dict["column_terms_character_length"] = (
                    column_terms_character_length
                )
                table_wise_analysis_dict[table_urn] = column_stats_dict
                table_wise_analysis_dict[table_urn].update(
                    get_classification_stats(table_urn, pred_df)
                )
            else:
                table_wise_analysis_dict[table_urn] = {
                    "column_terms_character_length": column_terms_character_length
                }
        except Exception as e:
            print(e)
            continue
    classification_report_df = pd.DataFrame(table_wise_analysis_dict).transpose()
    classification_report_df.index.names = ["table_urn"]
    classification_report_df = classification_report_df.reset_index()
    return classification_report_df


def func_categorize(row, label_column):
    if len(row["pred_max_score_term"]) == 0:
        if row[label_column] is None:
            return "match-no_assignment"
        elif "NULL" in row[label_column]:
            actual_terms = [
                term.strip() for term in row[label_column].split(GROUND_TRUTH_TERM_SEP)
            ]
            assigned_terms = [
                term[0]
                for term in row["predicted_labels"]
                if term[1] == max(i[1] for i in row["predicted_labels"])
            ]
            if len(np.intersect1d(actual_terms, assigned_terms)) > 0:
                return "match-no_assignment"
            else:
                return "mismatch-actual_only"
        else:
            return "mismatch-actual_only"
    elif row[label_column] is None:
        return "mismatch-predicted_only"
    else:
        actual_terms = [
            term.strip() for term in row[label_column].split(GROUND_TRUTH_TERM_SEP)
        ]
        if len(np.intersect1d(actual_terms, row["pred_max_score_term"])) == 0:
            return "mismatch"
        else:
            return "match-term_assigned"


def get_merged_prediction_df(labeled_df, df_pred, label_column):
    merged_df = pd.merge(
        labeled_df,
        df_pred,
        on="unique_keys",
        how="left",
    )
    merged_df.loc[:, "pred_max_score_term"] = merged_df["pred_max_score_term"].apply(
        lambda x: x if isinstance(x, list) else []
    )
    merged_df.loc[:, "predicted_labels"] = merged_df["predicted_labels"].apply(
        lambda x: x if isinstance(x, list) else []
    )
    not_omitted_columns_df = merged_df[
        merged_df.unique_keys.isin(df_pred.unique_keys.tolist())
    ]
    omitted_columns_df = merged_df[
        ~merged_df.unique_keys.isin(df_pred.unique_keys.tolist())
    ]

    merged_df.loc[:, "label_class"] = merged_df.apply(
        lambda x: func_categorize(x, label_column), axis=1
    )
    return merged_df, not_omitted_columns_df, omitted_columns_df


def get_parsed_responses_for_single_experiment_run(
    urns_dict, glossary_info, prompt_path
):
    raw_llm_responses = []
    parsed_llm_responses = []

    for instance, urns in urns_dict.items():
        graph_client = create_datahub_graph(instance)
        for urn in urns:
            print(urn)
            raw_llm_response = None
            try:
                table_terms, column_terms, raw_llm_response = get_term_recommendations(
                    table_urn=urn,
                    graph_client=graph_client,
                    glossary_info=glossary_info,
                    prompt_path=prompt_path,
                )
                #             column_terms = label_fake_terms(column_terms)
                raw_llm_responses.append([urn, instance, raw_llm_response])
                parsed_llm_responses.append((urn, instance, table_terms, column_terms))
            except Exception as e:
                logger.exception(f"Exception Occurred {e}")
                raw_llm_responses.append([urn, instance, raw_llm_response])
                parsed_llm_responses.append((urn, instance, None, None))
                continue
    return parsed_llm_responses, raw_llm_responses


def get_aggregated_metrics(classification_report_df, confidence_threshold):
    total_TP = classification_report_df.TP.sum()
    total_FP1 = classification_report_df.FP1.sum()
    total_FP2 = classification_report_df.FP2.sum()
    total_FN = classification_report_df.FN.sum()
    total_TN = classification_report_df.TN.sum()

    precision_term_for_all_tables = (
        total_TP / (total_TP + total_FP1 + total_FP2)
        if (total_TP + total_FP1 + total_FP2) != 0
        else np.nan
    )

    recall_term_for_all_tables = (
        total_TP / (total_TP + total_FN + total_FP2)
        if (total_TP + total_FN + total_FP2) != 0
        else np.nan
    )

    precision_none_for_all_tables = (
        total_TN / (total_TN + total_FN) if (total_TN + total_FN) != 0 else np.nan
    )

    recall_none_for_all_tables = (
        total_TN / (total_TN + total_FP1) if (total_TN + total_FP1) != 0 else np.nan
    )

    aggregated_metrics = {
        "precision_term_for_all_tables": precision_term_for_all_tables,
        "recall_term_for_all_tables": recall_term_for_all_tables,
        "precision_none_for_all_tables": precision_none_for_all_tables,
        "recall_none_for_all_tables": recall_none_for_all_tables,
        "threshold": confidence_threshold,
    }
    return aggregated_metrics


def get_final_statistics_for_confidence_threshold(
    merged_prediction_df,
    parsed_llm_responses,
    filters,
    columns_info_dict,
    labeled_df,
    confidence_threshold,
):
    final_stats = {}
    classification_reports = {}
    for filter_ in filters:
        filtered_prediction_df = filter_predictions_df(merged_prediction_df, filter_)
        classification_report_df = get_classification_report_df(
            parsed_llm_responses=parsed_llm_responses,
            pred_df=filtered_prediction_df,
            column_infos_dict=columns_info_dict,
            labeled_df=labeled_df,
            filter_=filter_,
        )
        classification_reports[filter_] = classification_report_df
        temp_stats = dict(
            classification_report_df[
                [
                    "fake_columns_count",
                    "fake_terms_count",
                    "actual_column_count",
                    "skipped_columns_count",
                    "TP",
                    "TN",
                    "FP1",
                    "FP2",
                    "FN",
                ]
            ].sum(axis=0)
        )
        aggregated_metrics = get_aggregated_metrics(
            classification_report_df=classification_report_df,
            confidence_threshold=confidence_threshold,
        )
        temp_stats.update(aggregated_metrics)
        final_stats[filter_] = temp_stats
    final_stats_df = pd.DataFrame(final_stats)
    return final_stats_df, classification_reports


def save_misclassification_analysis(
    merged_prediction_df, dest_dir, confidence_threshold, label_column
):
    miscalssification_analysis_cols = [
        "table_urn",
        "col_name",
        "col_description",
        "sample_values",
        label_column,
        "predicted_labels",
    ]

    merged_prediction_df[merged_prediction_df.label_class == "mismatch-actual_only"][
        miscalssification_analysis_cols
    ].to_csv(dest_dir / f"FN_threshold_{confidence_threshold}.csv")

    merged_prediction_df[merged_prediction_df.label_class == "mismatch-predicted_only"][
        miscalssification_analysis_cols
    ].to_csv(dest_dir / f"FP1_threshold_{confidence_threshold}.csv")

    merged_prediction_df[merged_prediction_df.label_class == "mismatch"][
        miscalssification_analysis_cols
    ].to_csv(dest_dir / f"FP2_threshold_{confidence_threshold}.csv")
    return None


def populate_analysis_for_confidence_threshold_list(
    output_dir,
    parsed_llm_responses,
    filters,
    urns_dict,
    columns_info_dict,
    confidence_thresholds,
    column_labels_csv_path,
    label_column,
):
    for confidence_threshold in confidence_thresholds:
        print("CONFIDENCE_THRESHOLD", confidence_threshold)

        # Make Directory
        SUB_DIR = output_dir / f"threshold_{confidence_threshold}"
        SUB_DIR.mkdir(parents=True, exist_ok=True)

        # Read labelled data
        labeled_df = read_labeled_column_data(column_labels_csv_path, urns_dict)
        tables_with_parsing_error, skipped_tables = get_failed_response_table_urns(
            parsed_llm_responses
        )
        print("tables_with_parsing_error:", tables_with_parsing_error)
        print("skipped_tables:", skipped_tables)

        # Prepare prediction df:
        df_pred = get_prediction_df(parsed_llm_responses, confidence_threshold)
        print("labeled_df", len(labeled_df))
        print("prediction_df", len(df_pred))

        # Merge Prediction df and labelled df
        merged_df, _, _ = get_merged_prediction_df(
            labeled_df[~labeled_df.table_urn.isin(tables_with_parsing_error)],
            df_pred,
            label_column,
        )
        print("merged_df", len(merged_df))
        merged_df.to_csv(
            SUB_DIR / f"predicted_labels_threshold_{confidence_threshold}.csv"
        )

        # Misclassification Analysis:
        save_misclassification_analysis(
            merged_df, SUB_DIR, confidence_threshold, label_column=label_column
        )

        # Final Statistics
        final_stats_df, classification_reports = (
            get_final_statistics_for_confidence_threshold(
                merged_prediction_df=merged_df,
                parsed_llm_responses=parsed_llm_responses,
                filters=filters,
                columns_info_dict=columns_info_dict,
                labeled_df=labeled_df,
                confidence_threshold=confidence_threshold,
            )
        )

        for key, classification_report_df in classification_reports.items():
            classification_report_df.to_csv(SUB_DIR / f"{key}_cf_df.csv")
        final_stats_df.to_csv(
            SUB_DIR / f"final_stats_threshold_{confidence_threshold}.csv"
        )
    return None


def convert_parsed_response_to_readable_csv(
    parsed_responses, columns_info_dict, destination_csv_path, threshold=9
):
    table_urns = []
    instances = []
    columns = []
    column_terms = []
    column_descriptions = []
    column_sample_values = []
    column_datatypes = []
    for table, instance, _, all_column_terms in parsed_responses:
        if all_column_terms is None:
            continue
        for column, terms in all_column_terms.items():
            if column in columns_info_dict[table].keys():
                table_urns.append(table)
                column_descriptions.append(
                    columns_info_dict[table][column].get("descriptions", "")
                )
                column_datatypes.append(
                    columns_info_dict[table][column]
                    .get("metadata", "")
                    .get("nativeDataType")
                )
                column_sample_values.append(
                    columns_info_dict[table][column].get("sample_values", [])
                )
                instances.append(instance)
                columns.append(column)
                column_terms.append(
                    [
                        (term.name, term.confidence_score, term.reasoning, term.is_fake)
                        for term in terms
                    ]
                )

    df = pd.DataFrame(
        {
            "urn": table_urns,
            "instance": instances,
            "column": columns,
            "datatype": column_datatypes,
            "description": column_descriptions,
            "sample_values": column_sample_values,
            "assigned_terms": column_terms,
        }
    )
    df["high_conf_terms"] = df.assigned_terms.apply(
        lambda x: [
            (term[0], term[1], term[2])
            for term in x
            if term[1] >= threshold and not term[3]
        ]
    )
    df.to_csv(destination_csv_path)
    return None


def get_average_run_scores_df(run_paths, scores_file_name):
    score_df = pd.DataFrame()
    for i, run_path in enumerate(run_paths):
        score_path = pathlib.Path(run_path) / "threshold_9" / f"{scores_file_name}"
        if i == 0:
            score_df = pd.read_csv(score_path)
            score_df.columns = ["index_col", os.path.basename(run_path).split("_")[0]]
        else:
            temp_df = pd.read_csv(score_path)
            score_df[os.path.basename(run_path).split("_")[0]] = temp_df["no_filter"]
    score_df["average"] = score_df.mean(
        numeric_only=True, axis=1
    )  # score_df.drop('index_col', axis=1).mean(axis=1)
    return score_df


def get_predictions_above_threshold(row, threshold):
    row = ast.literal_eval(row)
    if row:  # Ensure the list is not empty
        min_score = threshold
        return [x[0] for x in row if x[1] >= min_score]
    return None


def get_combined_data(
    run_paths, predictions_file_name, common_columns, num_iter=10, threshold=9.0
):
    combined_data = []
    for run_path in run_paths:
        file_path = pathlib.Path(run_path) / f"{predictions_file_name}"
        run = os.path.basename(run_path).split("_")[0]
        # Check if the file exists
        if os.path.exists(file_path):
            # Read the CSV file
            df = pd.read_csv(file_path, usecols=common_columns + ["high_conf_terms"])
            df = df.rename(columns={"high_conf_terms": run})
            combined_data.append(df)
        else:
            print(f"File not found: {file_path}")

    # Combine all data for this subdirectory and run
    if combined_data:
        # Perform the join
        combined_df = reduce(
            lambda left, right: pd.merge(left, right, on=common_columns, how="inner"),
            combined_data,
        )
        output_columns = [col for col in combined_df.columns if "run" in col]
        for output_column in output_columns:
            combined_df[f"{output_column}_pred"] = combined_df[output_column].apply(
                lambda x: get_predictions_above_threshold(x, threshold=threshold)
            )
    else:
        logger.info("Failed to get combined dataframes!!!")
        return None, None, None
    run_columns = [f"{x}" for x in combined_df.columns if "pred" in x]
    combined_df["predicted_label_counts"] = combined_df[run_columns].apply(
        lambda x: get_value_counts(x), axis=1
    )
    combined_df["all_similar"] = combined_df.predicted_label_counts.apply(
        lambda x: num_iter <= max(x.values())
    )
    combined_df["no_term"] = combined_df.predicted_label_counts.apply(
        lambda x: True if (len(x) == 1 and None in x.keys()) else False
    )
    combined_df["agg_category"] = combined_df.apply(
        lambda x: get_agg_category(x), axis=1
    )

    value_counts_df = pd.DataFrame(
        {
            "consistency_stats": dict(combined_df.agg_category.value_counts()),
        }
    )
    value_counts_df = value_counts_df.reindex(
        ["term_similar", "no_term_similar", "term_not_similar"]
    )
    return combined_df, value_counts_df


def get_value_counts(sample_row):
    temp_var = []
    # sample_row = df2[run_columns].loc[0]
    sample_row.apply(
        lambda x: temp_var.append(None) if x is None else temp_var.extend(x)
    )
    value_counts = pd.Series(temp_var).value_counts(dropna=False)
    return dict(value_counts)


def get_agg_category(x):
    if x.all_similar and x.no_term:
        return "no_term_similar"
    elif x.all_similar and not x.no_term:
        return "term_similar"
    elif not x.all_similar:
        return "term_not_similar"
    else:
        return "NA"


def write_llm_output_to_csv(
    llm_response: list, csv_path: str | pathlib.Path = ""
) -> None:
    serialized_responses = SerializedResponse(response_dict=llm_response).dict()[
        "response_dict"
    ]
    if csv_path == "":
        csv_path = (
            f"term_suggestion_output_{datetime.now().strftime('%m-%d-%Y_%H-%M-%S')}.csv"
        )
    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        csvwriter = csv.writer(csvfile)

        if len(serialized_responses[0]) == 4:
            csvwriter.writerow(
                ["urn", "instance", "table_glossary_terms", "column_glossary_terms"]
            )
        else:
            csvwriter.writerow(["urn", "instance", "raw_output"])

        for row in serialized_responses:
            row = list(row)
            if len(row) == 4:
                row[3] = json5.dumps(row[3], indent=2)
            csvwriter.writerow(row)
    logger.info(f"csv file {csv_path} created successfully")
