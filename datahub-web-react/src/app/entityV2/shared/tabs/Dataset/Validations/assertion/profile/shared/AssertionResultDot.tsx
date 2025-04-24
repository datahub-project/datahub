import React from 'react';
import styled from 'styled-components';

import { getResultDotIcon } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';

import { AssertionResultType, AssertionRunEvent } from '@types';

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
