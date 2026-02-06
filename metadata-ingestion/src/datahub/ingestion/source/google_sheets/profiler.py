import time
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from datahub.ingestion.source.google_sheets.config import GoogleSheetsSourceConfig
from datahub.ingestion.source.google_sheets.constants import (
    DEFAULT_PROFILING_MAX_ROWS,
    MAX_DISTINCT_VALUES_FOR_FREQUENCIES,
    MAX_SAMPLE_VALUES_COUNT,
)
from datahub.ingestion.source.google_sheets.models import parse_values_response
from datahub.ingestion.source.google_sheets.report import GoogleSheetsSourceReport
from datahub.metadata.schema_classes import (
    DatasetFieldProfileClass,
    DatasetProfileClass,
    HistogramClass,
    QuantileClass,
    ValueFrequencyClass,
)
from datahub.utilities.perf_timer import PerfTimer


class GoogleSheetsProfiler:
    def __init__(
        self,
        sheets_service: Any,
        config: GoogleSheetsSourceConfig,
        report: GoogleSheetsSourceReport,
        sheets_as_datasets: bool,
    ):
        self.sheets_service = sheets_service
        self.config = config
        self.report = report
        self.sheets_as_datasets = sheets_as_datasets

    def profile_sheet(
        self, sheet_id: str, sheet_data: Dict[str, Any]
    ) -> Optional[DatasetProfileClass]:
        if not self.config.profiling.enabled:
            return None

        with PerfTimer() as timer:
            profile = DatasetProfileClass(timestampMillis=int(time.time() * 1000))
            sheets = sheet_data["spreadsheet"].sheets

            total_row_count = 0
            field_profiles = []

            for sheet in sheets:
                sheet_name = sheet.properties.title

                max_rows = self.config.profiling.limit or DEFAULT_PROFILING_MAX_ROWS
                range_name = f"{sheet_name}!1:{max_rows + 1}"
                try:
                    result = (
                        self.sheets_service.spreadsheets()
                        .values()
                        .get(spreadsheetId=sheet_id, range=range_name)
                        .execute()
                    )

                    values_response = parse_values_response(result)
                    if not values_response.values:
                        continue

                    headers = values_response.values[0]
                    data_rows = values_response.values[1:]

                    df = pd.DataFrame(data_rows, columns=headers)
                    row_count = len(df)
                    total_row_count += row_count

                    columns_to_profile = self._get_columns_to_profile(df)

                    for col in columns_to_profile:
                        field_profile = self._profile_column(
                            df, col, sheet_name, row_count
                        )
                        field_profiles.append(field_profile)

                except Exception as e:
                    self.report.report_warning(
                        message="Error profiling sheet", context=sheet_name, exc=e
                    )

            profile.rowCount = total_row_count
            profile.columnCount = len(field_profiles)
            profile.fieldProfiles = field_profiles

            self.report.report_sheet_profiled()
            self.report.profile_sheet_timer[sheet_id] = timer.elapsed_seconds()
            return profile

    def profile_sheet_tab(
        self, sheet_id: str, sheet_data: Dict[str, Any], sheet_name: str
    ) -> Optional[DatasetProfileClass]:
        if not self.config.profiling.enabled:
            return None

        with PerfTimer() as timer:
            profile = DatasetProfileClass(timestampMillis=int(time.time() * 1000))
            field_profiles = []

            max_rows = self.config.profiling.limit or 10000
            range_name = f"{sheet_name}!1:{max_rows + 1}"
            try:
                result = (
                    self.sheets_service.spreadsheets()
                    .values()
                    .get(spreadsheetId=sheet_id, range=range_name)
                    .execute()
                )

                values_response = parse_values_response(result)
                if not values_response.values:
                    return None

                headers = values_response.values[0]
                data_rows = values_response.values[1:]

                df = pd.DataFrame(data_rows, columns=headers)
                row_count = len(df)

                columns_to_profile = self._get_columns_to_profile(df)

                for col in columns_to_profile:
                    field_profile = self._profile_column(
                        df,
                        col,
                        sheet_name if not self.sheets_as_datasets else "",
                        row_count,
                    )
                    if self.sheets_as_datasets:
                        field_profile.fieldPath = col
                    field_profiles.append(field_profile)

                profile.rowCount = row_count
                profile.columnCount = len(field_profiles)
                profile.fieldProfiles = field_profiles

                self.report.report_sheet_profiled()
                self.report.profile_sheet_timer[f"{sheet_id}/{sheet_name}"] = (
                    timer.elapsed_seconds()
                )
                return profile

            except Exception as e:
                self.report.report_warning(
                    message="Error profiling sheet tab", context=sheet_name, exc=e
                )
                return None

    def _get_columns_to_profile(self, df: pd.DataFrame) -> List[str]:
        columns_to_profile = list(df.columns)
        if (
            self.config.profiling.max_number_of_fields_to_profile is not None
            and len(columns_to_profile)
            > self.config.profiling.max_number_of_fields_to_profile
        ):
            columns_to_profile = columns_to_profile[
                : self.config.profiling.max_number_of_fields_to_profile
            ]
        return columns_to_profile

    def _profile_column(
        self, df: pd.DataFrame, col: str, sheet_name: str, row_count: int
    ) -> DatasetFieldProfileClass:
        field_path = f"{sheet_name}.{col}" if sheet_name else col
        field_profile = DatasetFieldProfileClass(fieldPath=field_path)

        try:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            is_numeric = True
        except (ValueError, TypeError):
            is_numeric = False

        non_null_count = df[col].count()
        null_count = row_count - non_null_count

        if self.config.profiling.include_field_null_count:
            field_profile.nullCount = null_count
            field_profile.nullProportion = (
                null_count / row_count if row_count > 0 else 0
            )

        unique_count = df[col].nunique()
        field_profile.uniqueCount = unique_count
        field_profile.uniqueProportion = (
            unique_count / non_null_count if non_null_count > 0 else 0
        )

        if is_numeric and non_null_count > 0:
            self._add_numeric_column_stats(field_profile, df, col, unique_count)

        return field_profile

    def _add_numeric_column_stats(
        self,
        field_profile: DatasetFieldProfileClass,
        df: pd.DataFrame,
        col: str,
        unique_count: int,
    ) -> None:
        if self.config.profiling.include_field_min_value:
            field_profile.min = str(df[col].min())

        if self.config.profiling.include_field_max_value:
            field_profile.max = str(df[col].max())

        if self.config.profiling.include_field_mean_value:
            field_profile.mean = str(df[col].mean())

        if self.config.profiling.include_field_median_value:
            field_profile.median = str(df[col].median())

        if self.config.profiling.include_field_stddev_value:
            field_profile.stdev = str(df[col].std())

        if self.config.profiling.include_field_quantiles:
            quantiles = [0.05, 0.25, 0.5, 0.75, 0.95]
            quantile_values = df[col].quantile(quantiles)
            field_profile.quantiles = [
                QuantileClass(quantile=str(q), value=str(quantile_values[q]))
                for q in quantiles
            ]

        if self.config.profiling.include_field_histogram:
            try:
                hist, bin_edges = np.histogram(df[col].dropna(), bins=20)
                field_profile.histogram = HistogramClass(
                    [str(bin_edges[i]) for i in range(len(bin_edges) - 1)],
                    [float(count) for count in hist],
                )
            except Exception as e:
                self.report.report_warning(
                    message="Error generating histogram", context=col, exc=e
                )

        if (
            self.config.profiling.include_field_distinct_value_frequencies
            and unique_count <= MAX_DISTINCT_VALUES_FOR_FREQUENCIES
        ):
            value_counts = (
                df[col].value_counts().head(MAX_DISTINCT_VALUES_FOR_FREQUENCIES)
            )
            field_profile.distinctValueFrequencies = [
                ValueFrequencyClass(value=str(value), frequency=int(count))
                for value, count in value_counts.items()
            ]

        if self.config.profiling.include_field_sample_values:
            non_null_count = df[col].count()
            sample_size = min(MAX_SAMPLE_VALUES_COUNT, non_null_count)
            if sample_size > 0:
                field_profile.sampleValues = [
                    str(val) for val in df[col].dropna().sample(sample_size).tolist()
                ]
