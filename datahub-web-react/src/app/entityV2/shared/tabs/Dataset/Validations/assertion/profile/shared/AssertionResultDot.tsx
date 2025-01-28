import React from 'react';
import styled from 'styled-components';

import { AssertionResultType, AssertionRunEvent } from '../../../../../../../../../types.generated';
import { getResultDotIcon } from '../../../assertionUtils';

const StyledAssertionResultDotContainer = styled.div`
    display: flex;
`;

type Props = {
    run?: AssertionRunEvent;
    disabled?: boolean;
    size?: number;
};

// TODO: Add our beautiful new tooltip here.
export const AssertionResultDot = ({ run, disabled, size = 14 }: Props) => {
    const icon = getResultDotIcon(run?.result?.type as AssertionResultType, size, disabled);
    return (
        <StyledAssertionResultDotContainer
            className="assertion-result-dot"
            data-assertion-resut-type={run?.result?.type}
        >
            {icon}
        </StyledAssertionResultDotContainer>
    );
};
