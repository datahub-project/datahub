import React from 'react';
import { Typography } from 'antd';
import { DatasetAssertionResultDetails } from '../../../../DatasetAssertionResultDetails';
import { getResultErrorMessage } from '../../../../assertionUtils';
import { AssertionResult, AssertionResultType } from '../../../../../../../../../../types.generated';

type Props = {
    result: AssertionResult;
};

export const TestAssertionResult = ({ result }: Props) => {
    if (result.type === AssertionResultType.Init) {
        return (
            <Typography.Text>
                No errors were found with your assertion, but we cannot test it out at this time, as more information is
                required to evaluate this assertion
            </Typography.Text>
        );
    }
    if (result.type === AssertionResultType.Error) {
        const errorMessage = getResultErrorMessage(result);
        const customMessage = result?.error?.properties?.find((property) => property.key === 'message');

        return customMessage ? (
            <Typography.Text>
                <b>Reason: </b>
                {customMessage.value}
            </Typography.Text>
        ) : (
            <Typography.Text>
                <b>Reason: </b>
                {errorMessage}
            </Typography.Text>
        );
    }

    return <DatasetAssertionResultDetails result={result} />;
};
