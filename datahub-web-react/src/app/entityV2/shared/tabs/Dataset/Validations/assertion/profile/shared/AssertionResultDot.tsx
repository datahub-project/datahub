import React from 'react';
import styled from 'styled-components';

import { getResultDotIconFromResultType } from '@app/entityV2/shared/tabs/Dataset/Validations/assertionUtils';

import { AssertionResultType } from '@types';

const StyledAssertionResultDotContainer = styled.div`
    display: flex;
`;

type Props = {
    resultType: AssertionResultType;
    disabled?: boolean;
    size?: number;
};

export const AssertionResultDot = ({ resultType, disabled, size = 14 }: Props) => {
    const icon = getResultDotIconFromResultType(resultType, size, disabled);
    return (
        <StyledAssertionResultDotContainer className="assertion-result-dot" data-assertion-result-type={resultType}>
            {icon}
        </StyledAssertionResultDotContainer>
    );
};
