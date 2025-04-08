import { Typography } from 'antd';
import React from 'react';

import { useBuildAssertionDescriptionLabels } from '@app/entityV2/shared/tabs/Dataset/Validations/assertion/profile/summary/utils';

import { Assertion } from '@types';

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
            <div>
                {!options?.hideSecondaryLabel && (
                    <Typography.Text style={{ marginLeft: options?.noSecondarySpacing ? 0 : 0 }}>
                        {secondaryLabel}
                    </Typography.Text>
                )}
            </div>
        </>
    );
};
