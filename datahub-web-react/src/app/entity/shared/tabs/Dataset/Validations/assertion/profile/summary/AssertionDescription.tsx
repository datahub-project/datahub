import React from 'react';

import { Assertion, Monitor } from '../../../../../../../../../types.generated';
import { useBuildAssertionDescriptionLabels } from './utils';

type Props = {
    assertion: Assertion;
    monitor?: Monitor;
};

// Component useful for rendering descriptions of assertions.
export const AssertionDescription = ({ assertion, monitor }: Props) => {
    const assertionInfo = assertion.info;
    const monitorSchedule = monitor?.info?.assertionMonitor?.assertions.find(
        (assrn) => assrn.assertion.urn === assertion.urn,
    )?.schedule;
    const { primaryLabel, secondaryLabel } = useBuildAssertionDescriptionLabels(assertionInfo, monitorSchedule);

    return (
        <>
            {primaryLabel}
            {secondaryLabel}
        </>
    );
};
