import React from 'react';
import { useTranslation } from 'react-i18next';
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

const MarkAsDeprecatedButtonContents = ({ internalText }: MarkAsDeprecatedButtonContentsProps) => {
    const { t } = useTranslation('entity.shared.components');
    return (
        <FlexContainer>
            <StyledDeprecatedIcon />
            {internalText || t('deprecation.markAsDeprecated')}
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
