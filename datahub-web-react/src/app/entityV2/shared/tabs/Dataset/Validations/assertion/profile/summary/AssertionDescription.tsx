import React from 'react';

import { Typography } from 'antd';
import { Assertion } from '../../../../../../../../../types.generated';
import { useBuildAssertionDescriptionLabels } from './utils';

type Props = {
    assertion: Assertion;
    options?: {
        noSecondarySpacing?: boolean;
        showColumnTag?: boolean;
        hideSecondaryLabel?: boolean;
    };
};

// Component useful for rendering descriptions of assertions.
export const AssertionDescription = ({ assertion, options }: Props) => {
    const assertionInfo = assertion.info;
    const monitorSchedule = null;
    const { primaryLabel, secondaryLabel } = useBuildAssertionDescriptionLabels(assertionInfo, monitorSchedule, {
        showColumnTag: options?.showColumnTag,
    });

    return (
        <>
            {primaryLabel}
            {!options?.hideSecondaryLabel && (
                <Typography.Text style={{ marginLeft: options?.noSecondarySpacing ? 0 : 12 }}>
                    {secondaryLabel}
                </Typography.Text>
            )}
        </>
    );
};
