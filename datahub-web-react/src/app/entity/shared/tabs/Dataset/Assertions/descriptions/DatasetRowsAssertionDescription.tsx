import React from 'react';
import { Typography } from 'antd';
import { DatasetRowsAssertion, DatasetRowsStdAggFunc, StringMapEntry } from '../../../../../../../types.generated';
import { convertParametersArrayToMap, getOpText } from './util';

type Props = {
    assertion: DatasetRowsAssertion;
    parameters: Array<StringMapEntry> | undefined;
};

export const DatasetRowsAssertionDescription = ({ assertion, parameters }: Props) => {
    const agg = assertion.stdAggFunc;
    const op = assertion.stdOperator;
    const parametersMap = convertParametersArrayToMap(parameters);

    const opText = getOpText(op, assertion.nativeType || undefined, parametersMap);
    let description: React.ReactNode;

    if (agg === DatasetRowsStdAggFunc.RowCount) {
        description = <Typography.Text>Dataset row count is {opText}</Typography.Text>;
    } else if (agg === DatasetRowsStdAggFunc.Native) {
        description = <Typography.Text>Assertion is {opText}</Typography.Text>;
    } else {
        throw new Error(`Unsupported Dataset Rows Aggregation ${agg} provided`);
    }

    return <>{description}</>;
};
