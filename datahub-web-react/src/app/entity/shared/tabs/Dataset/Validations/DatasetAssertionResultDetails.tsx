/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Typography } from 'antd';
import React from 'react';

import { AssertionResult } from '@types';

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
