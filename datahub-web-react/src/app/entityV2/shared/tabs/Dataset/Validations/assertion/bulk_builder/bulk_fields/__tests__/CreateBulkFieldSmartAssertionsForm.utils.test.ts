import {
    EligibleFieldColumn,
    getFieldMetricTypeOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils';
import { AssertionMonitorBuilderState } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/types';
import {
    SelectedColumnsAndMetrics,
    buildSelectedColumnsAndMetricsOnColumnsUpdate,
    buildSelectedColumnsAndMetricsOnMetricsUpdate,
    createBulkFieldAssertionSpecFromState,
    getColumnAndMetricOptions,
} from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/bulk_builder/bulk_fields/CreateBulkFieldSmartAssertionsForm.utils';

import {
    AssertionActionType,
    AssertionActionsInput,
    AssertionExclusionWindowType,
    CronSchedule,
    DatasetFieldAssertionSourceType,
    DayOfWeek,
    FieldMetricType,
    FreshnessFieldKind,
    SchemaFieldDataType,
} from '@types';

// Mock the getFieldMetricTypeOptions function
vi.mock('@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils', async () => {
    const actual = (await vi.importActual(
        '@app/entityV2/shared/tabs/Dataset/Validations/assertion/builder/steps/field/utils',
    )) as any;
    return {
        ...actual,
        getFieldMetricTypeOptions: vi.fn(),
    };
});

describe('getColumnAndMetricOptions', () => {
    // Helper function to create mock column
    const createMockColumn = (path: string, type: SchemaFieldDataType): EligibleFieldColumn => ({
        path,
        type,
        nativeType: 'varchar',
    });

    // Helper function to create mock metric option
    const createMockMetricOption = (value: FieldMetricType, label: string) => ({
        value,
        label,
    });

    // Helper function to create SelectedColumnsAndMetrics
    const createSelectedColumnsAndMetrics = (
        column: EligibleFieldColumn,
        metrics: FieldMetricType[],
    ): SelectedColumnsAndMetrics => ({
        column,
        metrics,
    });

    beforeEach(() => {
        vi.clearAllMocks();
    });

    describe('with single column and metrics', () => {
        it('should return correct structure for string column with basic metrics', () => {
            const stringColumn = createMockColumn('user_name', SchemaFieldDataType.String);
            const selectedColumnsAndMetrics = [
                createSelectedColumnsAndMetrics(stringColumn, [FieldMetricType.NullCount, FieldMetricType.UniqueCount]),
            ];

            const mockStringMetrics = [
                createMockMetricOption(FieldMetricType.NullCount, 'Null count'),
                createMockMetricOption(FieldMetricType.UniqueCount, 'Unique count'),
                createMockMetricOption(FieldMetricType.MaxLength, 'Max length'),
            ];

            (getFieldMetricTypeOptions as any).mockReturnValue(mockStringMetrics);

            const result = getColumnAndMetricOptions(
                selectedColumnsAndMetrics,
                DatasetFieldAssertionSourceType.AllRowsQuery,
            );

            expect(result.metricOptions).toEqual(mockStringMetrics);
            expect(result.availableMetricsForColumns).toEqual([
                {
                    columnName: 'user_name',
                    availableMetrics: mockStringMetrics,
                },
            ]);
            expect(result.validColumnsForEachMetric).toHaveLength(3);
            expect(result.validColumnsForEachMetric[0]).toEqual({
                metric: mockStringMetrics[0],
                columns: [{ columnName: 'user_name', availableMetrics: mockStringMetrics }],
            });
        });

        it('should handle empty selectedColumnsAndMetrics', () => {
            const result = getColumnAndMetricOptions([], DatasetFieldAssertionSourceType.AllRowsQuery);

            expect(result.metricOptions).toEqual([]);
            expect(result.availableMetricsForColumns).toEqual([]);
            expect(result.validColumnsForEachMetric).toEqual([]);
        });
    });

    describe('with multiple columns and different data types', () => {
        it('should return unique metrics across different column types', () => {
            const stringColumn = createMockColumn('user_name', SchemaFieldDataType.String);
            const numberColumn = createMockColumn('user_age', SchemaFieldDataType.Number);

            const selectedColumnsAndMetrics = [
                createSelectedColumnsAndMetrics(stringColumn, [FieldMetricType.NullCount]),
                createSelectedColumnsAndMetrics(numberColumn, [FieldMetricType.NullCount, FieldMetricType.Max]),
            ];

            const mockStringMetrics = [
                createMockMetricOption(FieldMetricType.NullCount, 'Null count'),
                createMockMetricOption(FieldMetricType.UniqueCount, 'Unique count'),
            ];

            const mockNumberMetrics = [
                createMockMetricOption(FieldMetricType.NullCount, 'Null count'),
                createMockMetricOption(FieldMetricType.Max, 'Max'),
                createMockMetricOption(FieldMetricType.Min, 'Min'),
            ];

            (getFieldMetricTypeOptions as any)
                .mockReturnValueOnce(mockStringMetrics)
                .mockReturnValueOnce(mockNumberMetrics);

            const result = getColumnAndMetricOptions(
                selectedColumnsAndMetrics,
                DatasetFieldAssertionSourceType.AllRowsQuery,
            );

            // Should have unique metrics (NullCount should appear only once)
            expect(result.metricOptions).toHaveLength(4);
            const metricValues = result.metricOptions.map((m) => m.value);
            expect(metricValues).toContain(FieldMetricType.NullCount);
            expect(metricValues).toContain(FieldMetricType.UniqueCount);
            expect(metricValues).toContain(FieldMetricType.Max);
            expect(metricValues).toContain(FieldMetricType.Min);

            expect(result.availableMetricsForColumns).toHaveLength(2);
            expect(result.availableMetricsForColumns[0].columnName).toBe('user_name');
            expect(result.availableMetricsForColumns[1].columnName).toBe('user_age');

            // Check validColumnsForEachMetric
            const nullCountMetric = result.validColumnsForEachMetric.find(
                (m) => m.metric.value === FieldMetricType.NullCount,
            );
            expect(nullCountMetric?.columns).toHaveLength(2); // Both columns support null count

            const maxMetric = result.validColumnsForEachMetric.find((m) => m.metric.value === FieldMetricType.Max);
            expect(maxMetric?.columns).toHaveLength(1); // Only number column supports max
            expect(maxMetric?.columns[0].columnName).toBe('user_age');
        });

        it('should correctly map columns to metrics they support', () => {
            const stringColumn = createMockColumn('description', SchemaFieldDataType.String);
            const booleanColumn = createMockColumn('is_active', SchemaFieldDataType.Boolean);

            const selectedColumnsAndMetrics = [
                createSelectedColumnsAndMetrics(stringColumn, [FieldMetricType.MaxLength]),
                createSelectedColumnsAndMetrics(booleanColumn, [FieldMetricType.NullCount]),
            ];

            const mockStringMetrics = [
                createMockMetricOption(FieldMetricType.NullCount, 'Null count'),
                createMockMetricOption(FieldMetricType.MaxLength, 'Max length'),
            ];

            const mockBooleanMetrics = [
                createMockMetricOption(FieldMetricType.NullCount, 'Null count'),
                createMockMetricOption(FieldMetricType.UniqueCount, 'Unique count'),
            ];

            (getFieldMetricTypeOptions as any)
                .mockReturnValueOnce(mockStringMetrics)
                .mockReturnValueOnce(mockBooleanMetrics);

            const result = getColumnAndMetricOptions(
                selectedColumnsAndMetrics,
                DatasetFieldAssertionSourceType.AllRowsQuery,
            );

            // Check that MaxLength metric only has string column
            const maxLengthMetric = result.validColumnsForEachMetric.find(
                (m) => m.metric.value === FieldMetricType.MaxLength,
            );
            expect(maxLengthMetric?.columns).toHaveLength(1);
            expect(maxLengthMetric?.columns[0].columnName).toBe('description');

            // Check that NullCount metric has both columns
            const nullCountMetric = result.validColumnsForEachMetric.find(
                (m) => m.metric.value === FieldMetricType.NullCount,
            );
            expect(nullCountMetric?.columns).toHaveLength(2);

            // Check that UniqueCount metric only has boolean column
            const uniqueCountMetric = result.validColumnsForEachMetric.find(
                (m) => m.metric.value === FieldMetricType.UniqueCount,
            );
            expect(uniqueCountMetric?.columns).toHaveLength(1);
            expect(uniqueCountMetric?.columns[0].columnName).toBe('is_active');
        });
    });

    describe('with different source types', () => {
        it('should pass sourceType to getFieldMetricTypeOptions', () => {
            const column = createMockColumn('test_col', SchemaFieldDataType.String);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(column, [FieldMetricType.NullCount])];

            const mockMetrics = [createMockMetricOption(FieldMetricType.NullCount, 'Null count')];
            (getFieldMetricTypeOptions as any).mockReturnValue(mockMetrics);

            getColumnAndMetricOptions(selectedColumnsAndMetrics, DatasetFieldAssertionSourceType.DatahubDatasetProfile);

            expect(getFieldMetricTypeOptions).toHaveBeenCalledWith(
                SchemaFieldDataType.String,
                DatasetFieldAssertionSourceType.DatahubDatasetProfile,
            );
        });

        it('should handle AllRowsQuery source type', () => {
            const column = createMockColumn('test_col', SchemaFieldDataType.Number);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(column, [FieldMetricType.Max])];

            const mockMetrics = [createMockMetricOption(FieldMetricType.Max, 'Max')];
            (getFieldMetricTypeOptions as any).mockReturnValue(mockMetrics);

            getColumnAndMetricOptions(selectedColumnsAndMetrics, DatasetFieldAssertionSourceType.AllRowsQuery);

            expect(getFieldMetricTypeOptions).toHaveBeenCalledWith(
                SchemaFieldDataType.Number,
                DatasetFieldAssertionSourceType.AllRowsQuery,
            );
        });
    });

    describe('edge cases', () => {
        it('should handle columns with no available metrics', () => {
            const column = createMockColumn('empty_col', SchemaFieldDataType.String);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(column, [])];

            (getFieldMetricTypeOptions as any).mockReturnValue([]);

            const result = getColumnAndMetricOptions(
                selectedColumnsAndMetrics,
                DatasetFieldAssertionSourceType.AllRowsQuery,
            );

            expect(result.metricOptions).toEqual([]);
            expect(result.availableMetricsForColumns).toHaveLength(1);
            expect(result.availableMetricsForColumns[0].availableMetrics).toEqual([]);
            expect(result.validColumnsForEachMetric).toEqual([]);
        });

        it('should handle duplicate metric types correctly with uniqBy', () => {
            const column1 = createMockColumn('col1', SchemaFieldDataType.String);
            const column2 = createMockColumn('col2', SchemaFieldDataType.String);

            const selectedColumnsAndMetrics = [
                createSelectedColumnsAndMetrics(column1, [FieldMetricType.NullCount]),
                createSelectedColumnsAndMetrics(column2, [FieldMetricType.NullCount]),
            ];

            const mockMetrics = [createMockMetricOption(FieldMetricType.NullCount, 'Null count')];
            (getFieldMetricTypeOptions as any).mockReturnValue(mockMetrics);

            const result = getColumnAndMetricOptions(
                selectedColumnsAndMetrics,
                DatasetFieldAssertionSourceType.AllRowsQuery,
            );

            // Should only have one instance of NullCount metric despite both columns having it
            expect(result.metricOptions).toHaveLength(1);
            expect(result.metricOptions[0].value).toBe(FieldMetricType.NullCount);

            // But both columns should be available for the NullCount metric
            const nullCountMetric = result.validColumnsForEachMetric[0];
            expect(nullCountMetric.columns).toHaveLength(2);
        });
    });
});

describe('createBulkFieldAssertionSpecFromState', () => {
    // Helper function to create mock schedule
    const createMockSchedule = (): CronSchedule => ({
        cron: '0 0 * * *',
        timezone: 'UTC',
    });

    // Helper function to create mock column
    const createMockColumn = (path: string, type: SchemaFieldDataType): EligibleFieldColumn => ({
        path,
        type,
        nativeType: 'varchar',
    });

    // Helper function to create SelectedColumnsAndMetrics
    const createSelectedColumnsAndMetrics = (
        column: EligibleFieldColumn,
        metrics: FieldMetricType[],
    ): SelectedColumnsAndMetrics => ({
        column,
        metrics,
    });

    describe('basic functionality', () => {
        it('should create spec with required parameters', () => {
            const entityUrn = 'urn:li:dataset:test';
            const column = createMockColumn('user_name', SchemaFieldDataType.String);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(column, [FieldMetricType.NullCount])];
            const schedule = createMockSchedule();
            const sourceType = DatasetFieldAssertionSourceType.AllRowsQuery;
            const inferenceSettings: AssertionMonitorBuilderState['inferenceSettings'] = undefined;

            const result = createBulkFieldAssertionSpecFromState({
                entityUrn,
                selectedColumnsAndMetrics,
                schedule,
                sourceType,
                inferenceSettings,
            });

            expect(result.entityUrn).toBe(entityUrn);
            expect(result.evaluationSchedule).toBe(schedule);
            expect(result.evaluationParameters.sourceType).toBe(sourceType);
            expect(result.fields).toHaveLength(1);
            expect(result.fields[0]).toEqual({
                field: column,
                metrics: [FieldMetricType.NullCount],
            });
        });

        it('should use default inference settings when not provided', () => {
            const entityUrn = 'urn:li:dataset:test';
            const column = createMockColumn('user_age', SchemaFieldDataType.Number);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(column, [FieldMetricType.Max])];
            const schedule = createMockSchedule();
            const sourceType = DatasetFieldAssertionSourceType.AllRowsQuery;
            const inferenceSettings: AssertionMonitorBuilderState['inferenceSettings'] = undefined;

            const result = createBulkFieldAssertionSpecFromState({
                entityUrn,
                selectedColumnsAndMetrics,
                schedule,
                sourceType,
                inferenceSettings,
            });

            expect(result.inferenceSettings?.sensitivity?.level).toBe(5); // DEFAULT_SMART_ASSERTION_SENSITIVITY
            expect(result.inferenceSettings?.trainingDataLookbackWindowDays).toBe(60); // DEFAULT_SMART_ASSERTION_TRAINING_LOOKBACK_WINDOW_DAYS
            expect(result.inferenceSettings?.exclusionWindows).toEqual([]);
        });

        it('should handle multiple columns and metrics', () => {
            const entityUrn = 'urn:li:dataset:test';
            const stringColumn = createMockColumn('user_name', SchemaFieldDataType.String);
            const numberColumn = createMockColumn('user_age', SchemaFieldDataType.Number);
            const selectedColumnsAndMetrics = [
                createSelectedColumnsAndMetrics(stringColumn, [FieldMetricType.NullCount, FieldMetricType.UniqueCount]),
                createSelectedColumnsAndMetrics(numberColumn, [FieldMetricType.Max, FieldMetricType.Min]),
            ];
            const schedule = createMockSchedule();
            const sourceType = DatasetFieldAssertionSourceType.AllRowsQuery;
            const inferenceSettings: AssertionMonitorBuilderState['inferenceSettings'] = undefined;

            const result = createBulkFieldAssertionSpecFromState({
                entityUrn,
                selectedColumnsAndMetrics,
                schedule,
                sourceType,
                inferenceSettings,
            });

            expect(result.fields).toHaveLength(2);
            expect(result.fields[0]).toEqual({
                field: stringColumn,
                metrics: [FieldMetricType.NullCount, FieldMetricType.UniqueCount],
            });
            expect(result.fields[1]).toEqual({
                field: numberColumn,
                metrics: [FieldMetricType.Max, FieldMetricType.Min],
            });
        });
    });

    describe('optional parameters', () => {
        it('should include changedRowsField when provided', () => {
            const entityUrn = 'urn:li:dataset:test';
            const column = createMockColumn('user_name', SchemaFieldDataType.String);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(column, [FieldMetricType.NullCount])];
            const schedule = createMockSchedule();
            const sourceType = DatasetFieldAssertionSourceType.AllRowsQuery;
            const changedRowsField = {
                path: 'updated_at',
                type: 'DATETIME',
                kind: FreshnessFieldKind.LastModified,
                nativeType: 'TIMESTAMP',
            };
            const inferenceSettings: AssertionMonitorBuilderState['inferenceSettings'] = undefined;

            const result = createBulkFieldAssertionSpecFromState({
                entityUrn,
                selectedColumnsAndMetrics,
                schedule,
                sourceType,
                changedRowsField,
                inferenceSettings,
            });

            expect(result.evaluationParameters.changedRowsField).toBe(changedRowsField);
        });

        it('should include actions when provided', () => {
            const entityUrn = 'urn:li:dataset:test';
            const column = createMockColumn('user_name', SchemaFieldDataType.String);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(column, [FieldMetricType.NullCount])];
            const schedule = createMockSchedule();
            const sourceType = DatasetFieldAssertionSourceType.AllRowsQuery;
            const actions: AssertionActionsInput = {
                onFailure: [
                    {
                        type: AssertionActionType.RaiseIncident,
                    },
                ],
                onSuccess: [],
            };
            const inferenceSettings: AssertionMonitorBuilderState['inferenceSettings'] = undefined;

            const result = createBulkFieldAssertionSpecFromState({
                entityUrn,
                selectedColumnsAndMetrics,
                schedule,
                sourceType,
                actions,
                inferenceSettings,
            });

            expect(result.actions).toBe(actions);
        });
    });

    describe('inference settings', () => {
        it('should use provided inference settings', () => {
            const entityUrn = 'urn:li:dataset:test';
            const column = createMockColumn('user_name', SchemaFieldDataType.String);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(column, [FieldMetricType.NullCount])];
            const schedule = createMockSchedule();
            const sourceType = DatasetFieldAssertionSourceType.AllRowsQuery;
            const inferenceSettings: AssertionMonitorBuilderState['inferenceSettings'] = {
                sensitivity: { level: 0.8 },
                trainingDataLookbackWindowDays: 60,
                exclusionWindows: [],
            };

            const result = createBulkFieldAssertionSpecFromState({
                entityUrn,
                selectedColumnsAndMetrics,
                schedule,
                sourceType,
                inferenceSettings,
            });

            expect(result.inferenceSettings?.sensitivity?.level).toBe(0.8);
            expect(result.inferenceSettings?.trainingDataLookbackWindowDays).toBe(60);
        });

        it('should handle exclusion windows', () => {
            const entityUrn = 'urn:li:dataset:test';
            const column = createMockColumn('user_name', SchemaFieldDataType.String);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(column, [FieldMetricType.NullCount])];
            const schedule = createMockSchedule();
            const sourceType = DatasetFieldAssertionSourceType.AllRowsQuery;
            const inferenceSettings: AssertionMonitorBuilderState['inferenceSettings'] = {
                sensitivity: { level: 0.5 },
                trainingDataLookbackWindowDays: 30,
                exclusionWindows: [
                    {
                        type: AssertionExclusionWindowType.Weekly,
                        displayName: 'Weekends',
                        weekly: {
                            daysOfWeek: ['SATURDAY' as DayOfWeek, 'SUNDAY' as DayOfWeek],
                            startTime: '00:00',
                            endTime: '23:59',
                            timezone: 'UTC',
                        },
                    },
                ],
            };

            const result = createBulkFieldAssertionSpecFromState({
                entityUrn,
                selectedColumnsAndMetrics,
                schedule,
                sourceType,
                inferenceSettings,
            });

            expect(result.inferenceSettings?.exclusionWindows).toHaveLength(1);
            expect(result.inferenceSettings?.exclusionWindows?.[0].type).toBe(AssertionExclusionWindowType.Weekly);
            expect(result.inferenceSettings?.exclusionWindows?.[0].displayName).toBe('Weekends');
            expect(result.inferenceSettings?.exclusionWindows?.[0].weekly?.daysOfWeek).toEqual(['SATURDAY', 'SUNDAY']);
        });
    });
});

describe('buildSelectedColumnsAndMetricsOnColumnsUpdate', () => {
    // Helper function to create mock column
    const createMockColumn = (path: string, type: SchemaFieldDataType): EligibleFieldColumn => ({
        path,
        type,
        nativeType: 'varchar',
    });

    // Helper function to create SelectedColumnsAndMetrics
    const createSelectedColumnsAndMetrics = (
        column: EligibleFieldColumn,
        metrics: FieldMetricType[],
    ): SelectedColumnsAndMetrics => ({
        column,
        metrics,
    });

    describe('basic functionality', () => {
        it('should return selected columns based on values with preserved metrics', () => {
            const col1 = createMockColumn('user_name', SchemaFieldDataType.String);
            const col2 = createMockColumn('user_age', SchemaFieldDataType.Number);
            const col3 = createMockColumn('user_email', SchemaFieldDataType.String);

            const columns = [col1, col2, col3];
            const selectedColumnsAndMetrics = [
                createSelectedColumnsAndMetrics(col1, [FieldMetricType.NullCount, FieldMetricType.UniqueCount]),
                createSelectedColumnsAndMetrics(col2, [FieldMetricType.Max, FieldMetricType.Min]),
            ];

            const values = ['user_name', 'user_email'];

            const result = buildSelectedColumnsAndMetricsOnColumnsUpdate(values, columns, selectedColumnsAndMetrics);

            expect(result).toHaveLength(2);
            expect(result[0].column.path).toBe('user_name');
            expect(result[0].metrics).toEqual([FieldMetricType.NullCount, FieldMetricType.UniqueCount]);
            expect(result[1].column.path).toBe('user_email');
            expect(result[1].metrics).toEqual([]); // New column, no existing metrics
        });

        it('should return empty array when no values provided', () => {
            const columns = [createMockColumn('user_name', SchemaFieldDataType.String)];
            const selectedColumnsAndMetrics: SelectedColumnsAndMetrics[] = [];

            const result = buildSelectedColumnsAndMetricsOnColumnsUpdate([], columns, selectedColumnsAndMetrics);

            expect(result).toEqual([]);
        });

        it('should return empty array when no matching columns found', () => {
            const columns = [createMockColumn('user_name', SchemaFieldDataType.String)];
            const selectedColumnsAndMetrics: SelectedColumnsAndMetrics[] = [];
            const values = ['non_existent_column'];

            const result = buildSelectedColumnsAndMetricsOnColumnsUpdate(values, columns, selectedColumnsAndMetrics);

            expect(result).toEqual([]);
        });
    });

    describe('deduplication', () => {
        it('should remove duplicate columns by path', () => {
            const col1 = createMockColumn('user_name', SchemaFieldDataType.String);
            const col1Duplicate = createMockColumn('user_name', SchemaFieldDataType.String);
            const col2 = createMockColumn('user_age', SchemaFieldDataType.Number);

            const columns = [col1, col1Duplicate, col2];
            const selectedColumnsAndMetrics: SelectedColumnsAndMetrics[] = [];
            const values = ['user_name', 'user_age'];

            const result = buildSelectedColumnsAndMetricsOnColumnsUpdate(values, columns, selectedColumnsAndMetrics);

            expect(result).toHaveLength(2);
            expect(result[0].column.path).toBe('user_name');
            expect(result[1].column.path).toBe('user_age');
        });
    });

    describe('metrics preservation', () => {
        it('should preserve existing metrics for columns that are still selected', () => {
            const col1 = createMockColumn('user_name', SchemaFieldDataType.String);
            const col2 = createMockColumn('user_age', SchemaFieldDataType.Number);

            const columns = [col1, col2];
            const selectedColumnsAndMetrics = [
                createSelectedColumnsAndMetrics(col1, [FieldMetricType.NullCount, FieldMetricType.UniqueCount]),
                createSelectedColumnsAndMetrics(col2, [FieldMetricType.Max]),
            ];

            const values = ['user_name']; // Only keeping user_name

            const result = buildSelectedColumnsAndMetricsOnColumnsUpdate(values, columns, selectedColumnsAndMetrics);

            expect(result).toHaveLength(1);
            expect(result[0].column.path).toBe('user_name');
            expect(result[0].metrics).toEqual([FieldMetricType.NullCount, FieldMetricType.UniqueCount]);
        });

        it('should assign empty metrics to new columns', () => {
            const col1 = createMockColumn('user_name', SchemaFieldDataType.String);
            const col2 = createMockColumn('user_age', SchemaFieldDataType.Number);

            const columns = [col1, col2];
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(col1, [FieldMetricType.NullCount])];

            const values = ['user_name', 'user_age']; // Adding user_age

            const result = buildSelectedColumnsAndMetricsOnColumnsUpdate(values, columns, selectedColumnsAndMetrics);

            expect(result).toHaveLength(2);
            expect(result[0].column.path).toBe('user_name');
            expect(result[0].metrics).toEqual([FieldMetricType.NullCount]);
            expect(result[1].column.path).toBe('user_age');
            expect(result[1].metrics).toEqual([]);
        });
    });
});

describe('buildSelectedColumnsAndMetricsOnMetricsUpdate', () => {
    // Helper function to create mock column
    const createMockColumn = (path: string, type: SchemaFieldDataType): EligibleFieldColumn => ({
        path,
        type,
        nativeType: 'varchar',
    });

    // Helper function to create SelectedColumnsAndMetrics
    const createSelectedColumnsAndMetrics = (
        column: EligibleFieldColumn,
        metrics: FieldMetricType[],
    ): SelectedColumnsAndMetrics => ({
        column,
        metrics,
    });

    // Helper function to create AvailableMetricForColumns
    const createAvailableMetricForColumns = (columnName: string, metrics: FieldMetricType[]) => ({
        columnName,
        availableMetrics: metrics.map((metric) => ({ value: metric, label: metric })),
    });

    describe('basic functionality', () => {
        it('should update metrics for all columns based on available metrics', () => {
            const col1 = createMockColumn('user_name', SchemaFieldDataType.String);
            const col2 = createMockColumn('user_age', SchemaFieldDataType.Number);

            const selectedColumnsAndMetrics = [
                createSelectedColumnsAndMetrics(col1, [FieldMetricType.NullCount]),
                createSelectedColumnsAndMetrics(col2, [FieldMetricType.Max]),
            ];

            const availableMetricsForColumns = [
                createAvailableMetricForColumns('user_name', [FieldMetricType.NullCount, FieldMetricType.UniqueCount]),
                createAvailableMetricForColumns('user_age', [
                    FieldMetricType.NullCount,
                    FieldMetricType.Max,
                    FieldMetricType.Min,
                ]),
            ];

            const values = [FieldMetricType.NullCount, FieldMetricType.Max];

            const result = buildSelectedColumnsAndMetricsOnMetricsUpdate(
                values,
                availableMetricsForColumns,
                selectedColumnsAndMetrics,
            );

            expect(result).toHaveLength(2);
            expect(result[0].column.path).toBe('user_name');
            expect(result[0].metrics).toEqual([FieldMetricType.NullCount]); // Only NullCount is available for string
            expect(result[1].column.path).toBe('user_age');
            expect(result[1].metrics).toEqual([FieldMetricType.NullCount, FieldMetricType.Max]); // Both are available for number
        });

        it('should return empty metrics when no values provided', () => {
            const col1 = createMockColumn('user_name', SchemaFieldDataType.String);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(col1, [FieldMetricType.NullCount])];
            const availableMetricsForColumns = [
                createAvailableMetricForColumns('user_name', [FieldMetricType.NullCount, FieldMetricType.UniqueCount]),
            ];

            const result = buildSelectedColumnsAndMetricsOnMetricsUpdate(
                [],
                availableMetricsForColumns,
                selectedColumnsAndMetrics,
            );

            expect(result).toHaveLength(1);
            expect(result[0].metrics).toEqual([]);
        });

        it('should handle empty selectedColumnsAndMetrics', () => {
            const availableMetricsForColumns = [
                createAvailableMetricForColumns('user_name', [FieldMetricType.NullCount]),
            ];

            const result = buildSelectedColumnsAndMetricsOnMetricsUpdate(
                [FieldMetricType.NullCount],
                availableMetricsForColumns,
                [],
            );

            expect(result).toEqual([]);
        });
    });

    describe('metric filtering', () => {
        it('should only assign metrics that are valid for each column', () => {
            const stringCol = createMockColumn('user_name', SchemaFieldDataType.String);
            const numberCol = createMockColumn('user_age', SchemaFieldDataType.Number);

            const selectedColumnsAndMetrics = [
                createSelectedColumnsAndMetrics(stringCol, []),
                createSelectedColumnsAndMetrics(numberCol, []),
            ];

            const availableMetricsForColumns = [
                createAvailableMetricForColumns('user_name', [FieldMetricType.NullCount, FieldMetricType.UniqueCount]),
                createAvailableMetricForColumns('user_age', [
                    FieldMetricType.NullCount,
                    FieldMetricType.Max,
                    FieldMetricType.Min,
                ]),
            ];

            const values = [FieldMetricType.NullCount, FieldMetricType.Max, FieldMetricType.UniqueCount];

            const result = buildSelectedColumnsAndMetricsOnMetricsUpdate(
                values,
                availableMetricsForColumns,
                selectedColumnsAndMetrics,
            );

            expect(result[0].column.path).toBe('user_name');
            expect(result[0].metrics).toEqual([FieldMetricType.NullCount, FieldMetricType.UniqueCount]);
            expect(result[1].column.path).toBe('user_age');
            expect(result[1].metrics).toEqual([FieldMetricType.NullCount, FieldMetricType.Max]);
        });

        it('should handle metrics not available for any column', () => {
            const col1 = createMockColumn('user_name', SchemaFieldDataType.String);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(col1, [])];
            const availableMetricsForColumns = [
                createAvailableMetricForColumns('user_name', [FieldMetricType.NullCount]),
            ];

            const values = [FieldMetricType.Max]; // Max not available for string column

            const result = buildSelectedColumnsAndMetricsOnMetricsUpdate(
                values,
                availableMetricsForColumns,
                selectedColumnsAndMetrics,
            );

            expect(result[0].metrics).toEqual([]);
        });
    });

    describe('deduplication', () => {
        it('should remove duplicate metrics', () => {
            const col1 = createMockColumn('user_name', SchemaFieldDataType.String);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(col1, [])];
            const availableMetricsForColumns = [
                createAvailableMetricForColumns('user_name', [FieldMetricType.NullCount, FieldMetricType.UniqueCount]),
            ];

            const values = [FieldMetricType.NullCount, FieldMetricType.NullCount, FieldMetricType.UniqueCount]; // Duplicate NullCount

            const result = buildSelectedColumnsAndMetricsOnMetricsUpdate(
                values,
                availableMetricsForColumns,
                selectedColumnsAndMetrics,
            );

            expect(result[0].metrics).toEqual([FieldMetricType.NullCount, FieldMetricType.UniqueCount]);
        });
    });

    describe('column not found in availableMetricsForColumns', () => {
        it('should assign empty metrics when column not found in availableMetricsForColumns', () => {
            const col1 = createMockColumn('unknown_column', SchemaFieldDataType.String);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(col1, [FieldMetricType.NullCount])];
            const availableMetricsForColumns = [
                createAvailableMetricForColumns('user_name', [FieldMetricType.NullCount]),
            ];

            const values = [FieldMetricType.NullCount];

            const result = buildSelectedColumnsAndMetricsOnMetricsUpdate(
                values,
                availableMetricsForColumns,
                selectedColumnsAndMetrics,
            );

            expect(result[0].column.path).toBe('unknown_column');
            expect(result[0].metrics).toEqual([]);
        });
    });

    describe('preserves column object', () => {
        it('should preserve the original column object', () => {
            const originalColumn = createMockColumn('user_name', SchemaFieldDataType.String);
            const selectedColumnsAndMetrics = [createSelectedColumnsAndMetrics(originalColumn, [])];
            const availableMetricsForColumns = [
                createAvailableMetricForColumns('user_name', [FieldMetricType.NullCount]),
            ];

            const values = [FieldMetricType.NullCount];

            const result = buildSelectedColumnsAndMetricsOnMetricsUpdate(
                values,
                availableMetricsForColumns,
                selectedColumnsAndMetrics,
            );

            expect(result[0].column).toBe(originalColumn); // Should be the same reference
        });
    });
});
