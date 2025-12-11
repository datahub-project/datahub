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
