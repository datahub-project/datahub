import React from 'react';

import { useBuildAssertionDescriptionLabels } from '@app/entity/shared/tabs/Dataset/Validations/assertion/profile/summary/utils';

import { Assertion, Monitor } from '@types';

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
