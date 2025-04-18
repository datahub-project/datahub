import React from 'react';
import { Typography } from 'antd';
import { DatasetAssertionResultDetails } from '../../../../DatasetAssertionResultDetails';
import { getResultErrorMessage } from '../../../../assertionUtils';
import { AssertionResult, AssertionResultType } from '../../../../../../../../../../types.generated';

type Props = {
    result: AssertionResult;
    isTest?: boolean;
};

export const RunAssertionResult = ({ result, isTest = false }: Props) => {
    if (result.type === AssertionResultType.Init) {
        return (
            <Typography.Text>
                {!isTest
                    ? 'This assertion is initialized. An assertion result will be available on the next run.'
                    : 'Unable to test this assertion because it requires more information to produce a passing or failing result.'}
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
