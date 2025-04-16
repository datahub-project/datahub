import React from 'react';
import styled from 'styled-components';
import { Button } from '@src/alchemy-components';
import DeprecatedIcon from '../../../../../images/deprecated-status.svg?react';

const StyledButton = styled(Button)`
    padding: 4px;
    margin-top: -8px;
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
        <StyledButton onClick={onClick} variant="text">
            <MarkAsDeprecatedButtonContents internalText={internalText} />
        </StyledButton>
    );
};

export default MarkAsDeprecatedButton;
