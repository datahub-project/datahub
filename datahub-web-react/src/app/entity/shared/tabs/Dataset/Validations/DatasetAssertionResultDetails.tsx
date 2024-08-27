import { Typography } from 'antd';
import React from 'react';
import { AssertionResult } from '../../../../../../types.generated';

type Props = {
    result: AssertionResult;
};

export const DatasetAssertionResultDetails = ({ result }: Props) => {
    const maybeActualValue = result.actualAggValue;
    const maybeUnexpectedCount = result.unexpectedCount;
    const maybeRowCount = result.rowCount;
    const maybeNativeResults = result.nativeResults;
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
