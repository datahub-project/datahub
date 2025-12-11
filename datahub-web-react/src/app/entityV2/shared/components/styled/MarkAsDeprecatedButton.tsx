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

import { Button } from '@src/alchemy-components';

import DeprecatedIcon from '@images/deprecated-status.svg?react';

const StyledButton = styled(Button)`
    padding-left: 4px;
    padding-right: 4px;
`;

const FlexContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
`;

const StyledDeprecatedIcon = styled(DeprecatedIcon)`
    color: inherit;
    path {
        fill: currentColor;
    }
    && {
        fill: currentColor;
    }
`;

type DeprecatedButtonProps = {
    onClick?: () => void;
    internalText?: string;
};

type MarkAsDeprecatedButtonContentsProps = {
    internalText?: string;
};

export const MarkAsDeprecatedButtonContents = ({ internalText }: MarkAsDeprecatedButtonContentsProps) => {
    return (
        <FlexContainer>
            <StyledDeprecatedIcon />
            {internalText || 'Mark as deprecated'}
        </FlexContainer>
    );
};

// Main Component
const MarkAsDeprecatedButton = ({ onClick, internalText }: DeprecatedButtonProps) => {
    return (
        <StyledButton onClick={onClick} variant="text" size="sm" color="gray">
            <MarkAsDeprecatedButtonContents internalText={internalText} />
        </StyledButton>
    );
};

export default MarkAsDeprecatedButton;
