import { Typography } from 'antd';
import React from 'react';
import { DatasetColumnAssertion, DatasetColumnStdAggFunc, StringMapEntry } from '../../../../../../../types.generated';
import { convertParametersArrayToMap, getOpText } from './util';

type Props = {
    fieldPath: string;
    assertion: DatasetColumnAssertion;
    parameters?: Array<StringMapEntry> | undefined;
};

export const DatasetColumnAssertionDescription = ({ fieldPath, assertion, parameters }: Props) => {
    const agg = assertion.stdAggFunc;
    const op = assertion.stdOperator;
    const parametersMap = convertParametersArrayToMap(parameters);

    const opText = getOpText(op, assertion.nativeOperator || undefined, parametersMap);
    let description: React.ReactNode;

    switch (agg) {
        // Hybrid Aggregations
        case DatasetColumnStdAggFunc.Identity: {
            // No aggregation on the column at hand. Treat the column as a set of values.
            description = (
                <Typography.Text>
                    Column <Typography.Text strong>{fieldPath}</Typography.Text> values are {opText}
                </Typography.Text>
            );
            break;
        }
        case DatasetColumnStdAggFunc.UniqueCount: {
            description = (
                <Typography.Text>
                    Unique value count for column <Typography.Text strong>{fieldPath}</Typography.Text> is {opText}
                </Typography.Text>
            );
            break;
        }
        case DatasetColumnStdAggFunc.UniquePropotion: {
            description = (
                <Typography.Text>
                    Unique value proportion for column <Typography.Text strong>{fieldPath}</Typography.Text> is {opText}
                </Typography.Text>
            );
            break;
        }
        case DatasetColumnStdAggFunc.NullCount: {
            description = (
                <Typography.Text>
                    Null count for column <Typography.Text strong>{fieldPath}</Typography.Text> is {opText}
                </Typography.Text>
            );
            break;
        }
        case DatasetColumnStdAggFunc.NullProportion: {
            description = (
                <Typography.Text>
                    Null proportion for column <Typography.Text strong>{fieldPath}</Typography.Text> is {opText}
                </Typography.Text>
            );
            break;
        }
        // Numeric Aggregations
        case DatasetColumnStdAggFunc.Min: {
            description = (
                <Typography.Text>
                    Minimum value for column <Typography.Text strong>{fieldPath}</Typography.Text> is {opText}
                </Typography.Text>
            );
            break;
        }
        case DatasetColumnStdAggFunc.Max: {
            description = (
                <Typography.Text>
                    Maximum value for column <Typography.Text strong>{fieldPath}</Typography.Text> is {opText}
                </Typography.Text>
            );
            break;
        }
        case DatasetColumnStdAggFunc.Mean: {
            description = (
                <Typography.Text>
                    Mean value for column <Typography.Text strong>{fieldPath}</Typography.Text> is {opText}
                </Typography.Text>
            );
            break;
        }
        case DatasetColumnStdAggFunc.Median: {
            description = (
                <Typography.Text>
                    Median value for column <Typography.Text strong>{fieldPath}</Typography.Text> is {opText}
                </Typography.Text>
            );
            break;
        }
        case DatasetColumnStdAggFunc.Stddev: {
            description = (
                <Typography.Text>
                    Standard deviation for column <Typography.Text strong>{fieldPath}</Typography.Text> is {opText}
                </Typography.Text>
            );
            break;
        }
        // Native Aggregations
        case DatasetColumnStdAggFunc.Native: {
            const nativeAgg = assertion.nativeAggFunc;
            description = (
                <Typography.Text>
                    {nativeAgg} for column <Typography.Text strong>{fieldPath}</Typography.Text> is {opText}
                </Typography.Text>
            );
            break;
        }
        default:
            throw new Error(`Unsupported Dataset Column Aggregation ${agg} provided`);
    }

    return <>{description}</>;
};
