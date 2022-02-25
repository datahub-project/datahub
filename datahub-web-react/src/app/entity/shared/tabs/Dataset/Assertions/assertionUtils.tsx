import React from 'react';
import { CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import {
    AssertionInfo,
    AssertionResultType,
    AssertionRunEvent,
    AssertionType,
    DatasetAssertionInfo,
    DatasetAssertionScope,
    DatasetColumnAssertion,
    DatasetRowsAssertion,
    DatasetRowsStdAggFunc,
    DatasetSchemaAssertion,
    DatasetSchemaStdAggFunc,
} from '../../../../../../types.generated';

/**
 * Assertion Run Event Results
 */

export const getResultText = (result: AssertionResultType) => {
    switch (result) {
        case AssertionResultType.Success:
            return 'Passed';
        case AssertionResultType.Failure:
            return 'Failed';
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};

export const getResultColor = (result: AssertionResultType) => {
    switch (result) {
        case AssertionResultType.Success:
            return 'green';
        case AssertionResultType.Failure:
            return 'red';
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};

export const getResultIcon = (result: AssertionResultType) => {
    const resultColor = getResultColor(result);
    switch (result) {
        case AssertionResultType.Success:
            return <CheckCircleOutlined style={{ color: resultColor }} />;
        case AssertionResultType.Failure:
            return <CloseCircleOutlined style={{ color: resultColor }} />;
        default:
            throw new Error(`Unsupported Assertion Result Type ${result} provided.`);
    }
};

const getDatasetNativeResultMessage = (runEvent: AssertionRunEvent) => {
    const maybeActualValue = runEvent.result?.actualAggValue;
    const maybeUnexpectedCount = runEvent.result?.unexpectedCount;
    const maybeRowCount = runEvent.result?.rowCount;
    const maybeNativeResults = runEvent.result?.nativeResults;
    return (
        <>
            {maybeActualValue !== null && maybeActualValue !== undefined && (
                <div>
                    <Typography.Text strong>Actual</Typography.Text>: {maybeActualValue}
                </div>
            )}
            {maybeUnexpectedCount !== null && maybeUnexpectedCount !== undefined && (
                <div>
                    <Typography.Text strong>Invalid Count</Typography.Text>: {maybeUnexpectedCount}
                </div>
            )}
            {maybeRowCount !== null && maybeRowCount !== undefined && (
                <div>
                    <Typography.Text strong>Row Count</Typography.Text>: {maybeRowCount}
                </div>
            )}
            {maybeNativeResults && (
                <div>
                    {maybeNativeResults.map((entry) => (
                        <div>
                            <Typography.Text strong>{entry.key}</Typography.Text>: {entry.value}
                        </div>
                    ))}
                </div>
            )}
        </>
    );
};

const getDatasetRowCountResultMessage = (runEvent: AssertionRunEvent) => {
    const maybeActualRowCount = runEvent.result?.rowCount;
    return (
        <>
            Row count = <Typography.Text strong>{maybeActualRowCount || 'N/A'}</Typography.Text>
        </>
    );
};

const getDatasetColumnCountResultMessage = (runEvent: AssertionRunEvent) => {
    return getDatasetNativeResultMessage(runEvent);
};

const getDatasetColumnsResultMessage = (runEvent: AssertionRunEvent) => {
    return getDatasetNativeResultMessage(runEvent);
};

const getDatasetRowsResultMessage = (
    assertion: DatasetRowsAssertion | null | undefined,
    runEvent: AssertionRunEvent,
) => {
    if (assertion) {
        switch (assertion.stdAggFunc) {
            case DatasetRowsStdAggFunc.RowCount: {
                return getDatasetRowCountResultMessage(runEvent);
            }
            case DatasetRowsStdAggFunc.Native:
            default: {
                return getDatasetNativeResultMessage(runEvent);
            }
        }
    }
    throw new Error("Invalid dataset rows assertion found: missing 'datasetRowsAssertion' field.");
};

const getDatasetSchemaResultMessage = (
    assertion: DatasetSchemaAssertion | null | undefined,
    runEvent: AssertionRunEvent,
) => {
    if (assertion) {
        switch (assertion.stdAggFunc) {
            case DatasetSchemaStdAggFunc.Columns: {
                return getDatasetColumnsResultMessage(runEvent);
            }
            case DatasetSchemaStdAggFunc.ColumnCount: {
                return getDatasetColumnCountResultMessage(runEvent);
            }
            case DatasetSchemaStdAggFunc.Native:
            default: {
                return getDatasetNativeResultMessage(runEvent);
            }
        }
    }
    throw new Error("Invalid dataset schema assertion found: missing 'datasetSchemaAssertion' field.");
};

const getDatasetColumnResultMessage = (
    assertion: DatasetColumnAssertion | null | undefined,
    runEvent: AssertionRunEvent,
) => {
    if (assertion) {
        return getDatasetNativeResultMessage(runEvent);
    }
    throw new Error("Invalid dataset column assertion found: missing 'datasetColumnAssertion' field.");
};

const getDatasetResultMessage = (
    datasetAssertionInfo: DatasetAssertionInfo | null | undefined,
    runEvent: AssertionRunEvent,
) => {
    if (datasetAssertionInfo) {
        switch (datasetAssertionInfo.scope) {
            case DatasetAssertionScope.DatasetRows: {
                return getDatasetRowsResultMessage(datasetAssertionInfo.rowsAssertion, runEvent);
            }
            case DatasetAssertionScope.DatasetSchema: {
                return getDatasetSchemaResultMessage(datasetAssertionInfo.schemaAssertion, runEvent);
            }
            case DatasetAssertionScope.DatasetColumn: {
                return getDatasetColumnResultMessage(datasetAssertionInfo.columnAssertion, runEvent);
            }
            default:
                throw new Error(`Unrecognized Dataset Assertion scope ${datasetAssertionInfo.scope} provided.`);
        }
    }
    throw new Error("Invalid dataset assertion found: missing 'daatasetAssertionInfo' field.");
};

export const getResultMessage = (assertionInfo: AssertionInfo, runEvent: AssertionRunEvent) => {
    if (assertionInfo && runEvent) {
        if (assertionInfo.type === AssertionType.Dataset) {
            // Only Dataset assertions are currently supported.
            return getDatasetResultMessage(assertionInfo.datasetAssertion, runEvent);
        }
        throw new Error(`Unrecognized assertion type ${assertionInfo.type} provided.`);
    }
    return null;
};
