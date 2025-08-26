import { Typography } from 'antd';
import React from 'react';

import { DatasetAssertionResultDetails } from '@app/entity/shared/tabs/Dataset/Validations/DatasetAssertionResultDetails';
import { getResultErrorMessage } from '@app/entity/shared/tabs/Dataset/Validations/assertionUtils';

import { AssertionResult, AssertionResultType } from '@types';

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
