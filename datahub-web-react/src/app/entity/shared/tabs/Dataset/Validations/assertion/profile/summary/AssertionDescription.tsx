import React from 'react';

import {
    Assertion,
} from '../../../../../../../../../types.generated';
import { useBuildAssertionDescriptionLabels } from './utils';

type Props = {
    assertion: Assertion;
};

// Component useful for rendering descriptions of assertions.
export const AssertionDescription = ({ assertion }: Props) => {
    const assertionInfo = assertion.info;
    const { primaryLabel, secondaryLabel } = useBuildAssertionDescriptionLabels(assertionInfo);

    return (
        <>
            {primaryLabel}
            {secondaryLabel}
        </>
    );
};
