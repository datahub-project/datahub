/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
