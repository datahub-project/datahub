import pathlib
import sys
from typing import Dict, List

import numpy as np
import pandas as pd

from datahub_integrations.gen_ai.description_v2 import (
    extract_metadata_for_urn,
    transform_table_info_for_llm,
)

current_dir = pathlib.Path().parent.resolve()
sys.path.append(str(current_dir.parent))
from docs_generation.graph_helper import create_datahub_graph


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
        instance, table_urn = response[1], response[0]
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
            if (term[1] == max([i[1] for i in x])) and (term[1] >= confidence_threshold)
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
