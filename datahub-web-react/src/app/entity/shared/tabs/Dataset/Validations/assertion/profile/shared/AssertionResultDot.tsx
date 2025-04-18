import React from 'react';

import { AssertionResultType, AssertionRunEvent } from '../../../../../../../../../types.generated';
import { getResultDotIcon } from '../../../assertionUtils';

type Props = {
    run?: AssertionRunEvent;
    disabled?: boolean;
    size?: number;
};

// TODO: Add our beautiful new tooltip here.
export const AssertionResultDot = ({ run, disabled, size = 14 }: Props) => {
    const icon = getResultDotIcon(run?.result?.type as AssertionResultType, size, disabled);
    return (
        <div className="assertion-result-dot" data-assertion-resut-type={run?.result?.type}>
            {icon}
        </div>
    );
};
