import { Popover, Typography, Button } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import {
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionStdParameters,
    DatasetAssertionInfo,
    DatasetAssertionScope,
    SchemaFieldRef,
} from '../../../../../../types.generated';
import { getFormattedParameterValue } from './assertionUtils';
import { DatasetAssertionLogicModal } from './DatasetAssertionLogicModal';

const ViewLogicButton = styled(Button)`
    padding: 0px;
    margin: 0px;
`;

type Props = {
    assertionInfo: DatasetAssertionInfo;
};

/**
 * Returns the React Component to render for the aggregation portion of the Assertion Description
 * for Assertions on Dataset Schemas.
 *
 * Schema assertions require an aggregation.
 */
const getSchemaAggregationText = (
    aggregation: AssertionStdAggregation | undefined | null,
    fields: Array<SchemaFieldRef> | undefined | null,
) => {
    switch (aggregation) {
        case AssertionStdAggregation.ColumnCount:
            return <Typography.Text>Dataset column count is</Typography.Text>;
        case AssertionStdAggregation.Columns:
            return <Typography.Text>Dataset columns are</Typography.Text>;
        case AssertionStdAggregation.Native: {
            const fieldNames = fields?.map((field) => field.path) || [];
            return (
                <Typography.Text>
                    Dataset columns <Typography.Text strong>{JSON.stringify(fieldNames)}</Typography.Text> are
                </Typography.Text>
            );
        }
        default:
            console.error(`Unsupported schema aggregation assertion ${aggregation} provided.`);
            return <Typography.Text>Dataset columns are</Typography.Text>;
    }
};

/**
 * Returns the React Component to render for the aggregation portion of the Assertion Description
 * for Assertions on Dataset Rows
 *
 * Row assertions require an aggregation.
 */
const getRowsAggregationText = (aggregation: AssertionStdAggregation | undefined | null) => {
    switch (aggregation) {
        case AssertionStdAggregation.RowCount:
            return <Typography.Text>Dataset row count is</Typography.Text>;
        case AssertionStdAggregation.Native:
            return <Typography.Text>Dataset rows are</Typography.Text>;
        default:
            console.error(`Unsupported Dataset Rows Aggregation ${aggregation} provided`);
            return <Typography.Text>Dataset rows are</Typography.Text>;
    }
};

/**
 * Returns the React Component to render for the aggregation portion of the Assertion Description
 * for Assertions on Dataset Columns
 */
const getColumnAggregationText = (
    aggregation: AssertionStdAggregation | undefined | null,
    field: SchemaFieldRef | undefined,
) => {
    let columnText = field?.path;
    if (field === undefined) {
        columnText = 'undefined';
        console.error(`Invalid field provided for Dataset Assertion with scope Column ${JSON.stringify(field)}`);
    }
    switch (aggregation) {
        // Hybrid Aggregations
        case AssertionStdAggregation.UniqueCount: {
            return (
                <Typography.Text>
                    Unique value count for column <Typography.Text strong>{columnText}</Typography.Text> is
                </Typography.Text>
            );
        }
        case AssertionStdAggregation.UniquePropotion: {
            return (
                <Typography.Text>
                    Unique value proportion for column <Typography.Text strong>{columnText}</Typography.Text> is
                </Typography.Text>
            );
        }
        case AssertionStdAggregation.NullCount: {
            return (
                <Typography.Text>
                    Null count for column <Typography.Text strong>{columnText}</Typography.Text> is
                </Typography.Text>
            );
        }
        case AssertionStdAggregation.NullProportion: {
            return (
                <Typography.Text>
                    Null proportion for column <Typography.Text strong>{columnText}</Typography.Text> is
                </Typography.Text>
            );
        }
        // Numeric Aggregations
        case AssertionStdAggregation.Min: {
            return (
                <Typography.Text>
                    Minimum value for column <Typography.Text strong>{columnText}</Typography.Text> is
                </Typography.Text>
            );
        }
        case AssertionStdAggregation.Max: {
            return (
                <Typography.Text>
                    Maximum value for column <Typography.Text strong>{columnText}</Typography.Text> is
                </Typography.Text>
            );
        }
        case AssertionStdAggregation.Mean: {
            return (
                <Typography.Text>
                    Mean value for column <Typography.Text strong>{columnText}</Typography.Text> is
                </Typography.Text>
            );
        }
        case AssertionStdAggregation.Median: {
            return (
                <Typography.Text>
                    Median value for column <Typography.Text strong>{columnText}</Typography.Text> is
                </Typography.Text>
            );
        }
        case AssertionStdAggregation.Stddev: {
            return (
                <Typography.Text>
                    Standard deviation for column <Typography.Text strong>{columnText}</Typography.Text> is
                </Typography.Text>
            );
        }
        // Native Aggregations
        case AssertionStdAggregation.Native: {
            return (
                <Typography.Text>
                    Column <Typography.Text strong>{columnText}</Typography.Text> values are
                </Typography.Text>
            );
        }
        default:
            // No aggregation on the column at hand. Treat the column as a set of values.
            return (
                <Typography.Text>
                    Column <Typography.Text strong>{columnText}</Typography.Text> values are
                </Typography.Text>
            );
    }
};

/**
 * Returns the React Component to render for the aggregation portion of the Assertion Description
 */
const getAggregationText = (
    scope: DatasetAssertionScope,
    aggregation: AssertionStdAggregation | undefined | null,
    fields: Array<SchemaFieldRef> | undefined | null,
) => {
    switch (scope) {
        case DatasetAssertionScope.DatasetSchema:
            return getSchemaAggregationText(aggregation, fields);
        case DatasetAssertionScope.DatasetRows:
            return getRowsAggregationText(aggregation);
        case DatasetAssertionScope.DatasetColumn:
            return getColumnAggregationText(aggregation, fields?.length === 1 ? fields[0] : undefined);
        default:
            console.error(`Unsupported Dataset Assertion scope ${scope} provided`);
            return 'Dataset is';
    }
};

/**
 * Returns the React Component to render for the operator portion of the Assertion Description
 */
const getOperatorText = (
    op: AssertionStdOperator,
    parameters: AssertionStdParameters | undefined,
    nativeType: string | undefined,
) => {
    switch (op) {
        // Hybrid Operators
        case AssertionStdOperator.Between: {
            return (
                <Typography.Text>
                    between{' '}
                    <Typography.Text strong>{getFormattedParameterValue(parameters?.minValue)} </Typography.Text>
                    and <Typography.Text strong>{getFormattedParameterValue(parameters?.maxValue)}</Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.EqualTo: {
            const operatorText = 'equal to';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(parameters?.value)}</Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.Contain: {
            const operatorText = 'contains';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(parameters?.value)} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.In: {
            const operatorText = 'in';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(parameters?.value)} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.NotNull: {
            const operatorText = 'not null';
            return <Typography.Text strong>{operatorText}</Typography.Text>;
        }
        // Numeric Operators
        case AssertionStdOperator.GreaterThan: {
            const operatorText = 'greater than';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(parameters?.value)} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.GreaterThanOrEqualTo: {
            const operatorText = 'greater than or equal to';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(parameters?.value)} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.LessThan: {
            const operatorText = 'less than';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(parameters?.value)} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.LessThanOrEqualTo: {
            const operatorText = 'less than or equal to';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(parameters?.value)} </Typography.Text>
                </Typography.Text>
            );
        }
        // String Operators
        case AssertionStdOperator.StartWith: {
            const operatorText = 'starts with';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(parameters?.value)}</Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.EndWith: {
            const operatorText = 'ends with';
            return (
                <Typography.Text>
                    {operatorText}{' '}
                    <Typography.Text strong>{getFormattedParameterValue(parameters?.value)} </Typography.Text>
                </Typography.Text>
            );
        }
        case AssertionStdOperator.Native: {
            return (
                <Typography.Text>
                    passing assertion <Typography.Text strong>{nativeType}</Typography.Text>
                </Typography.Text>
            );
        }
        default:
            return (
                <Typography.Text>
                    passing operator{' '}
                    <Typography.Text strong>
                        {op} with value ${getFormattedParameterValue(parameters?.value)}
                    </Typography.Text>
                </Typography.Text>
            );
    }
};

const TOOLTIP_MAX_WIDTH = 440;

/**
 * A human-readable description of an Assertion.
 *
 * For example, Column 'X' values are in [1, 2, 3]
 */
export const DatasetAssertionDescription = ({ assertionInfo }: Props) => {
    const { scope, aggregation, fields, operator, parameters, nativeType, nativeParameters, logic } = assertionInfo;
    const [isLogicVisible, setIsLogicVisible] = useState(false);
    /**
     * Build a description component from a) input (aggregation, inputs) b) the operator text
     */
    const description = (
        <>
            <Typography.Text>
                {getAggregationText(scope, aggregation, fields)}{' '}
                {getOperatorText(operator, parameters || undefined, nativeType || undefined)}
            </Typography.Text>
        </>
    );

    return (
        <Popover
            overlayStyle={{ maxWidth: TOOLTIP_MAX_WIDTH }}
            title={<Typography.Text strong>Details</Typography.Text>}
            content={
                <>
                    <Typography.Text strong>type</Typography.Text>: {nativeType || 'N/A'}
                    {nativeParameters?.map((parameter) => (
                        <div>
                            <Typography.Text strong>{parameter.key}</Typography.Text>: {parameter.value}
                        </div>
                    ))}
                </>
            }
        >
            <div>{description}</div>
            {logic && (
                <div>
                    <ViewLogicButton onClick={() => setIsLogicVisible(true)} type="link">
                        View Logic
                    </ViewLogicButton>
                </div>
            )}
            <DatasetAssertionLogicModal
                logic={logic || 'N/A'}
                visible={isLogicVisible}
                onClose={() => setIsLogicVisible(false)}
            />
        </Popover>
    );
};
