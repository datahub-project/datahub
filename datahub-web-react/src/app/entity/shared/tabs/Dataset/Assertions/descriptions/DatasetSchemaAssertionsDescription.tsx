import { Typography } from 'antd';
import React from 'react';
import { DatasetSchemaAssertion, DatasetSchemaStdAggFunc, StringMapEntry } from '../../../../../../../types.generated';
import { convertParametersArrayToMap, getOpText } from './util';

type Props = {
    assertion: DatasetSchemaAssertion;
    parameters?: Array<StringMapEntry> | undefined;
};

export const DatasetSchemaAssertionDescription = ({ assertion, parameters }: Props) => {
    const agg = assertion.stdAggFunc;
    const op = assertion.stdOperator;
    const parametersMap = convertParametersArrayToMap(parameters);

    const opText = getOpText(op, assertion.nativeOperator || undefined, parametersMap);
    let description: React.ReactNode;

    switch (agg) {
        case DatasetSchemaStdAggFunc.ColumnCount:
            // No aggregation on the column at hand. Treat the column as a set of values.
            description = <Typography.Text>Dataset column count is {opText}</Typography.Text>;
            break;
        case DatasetSchemaStdAggFunc.Columns:
            // TODO: Determine why this is considered an aggregation.
            description = <Typography.Text>Dataset columns are {opText}</Typography.Text>;
            break;
        default:
            throw new Error(`Unsupported schema aggregation assertion ${agg} provided.`);
    }

    return <>{description}</>;
};
