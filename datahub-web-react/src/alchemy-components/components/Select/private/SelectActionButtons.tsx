import React, { useCallback } from 'react';
import styled from 'styled-components';

import { Button } from '@components/components/Button';
import { ActionButtonsContainer, StyledIcon } from '@components/components/Select/components';
import { ActionButtonsProps } from '@components/components/Select/types';

import { shadows } from '@src/alchemy-components/theme';

export const StyledClearButton = styled(Button).attrs({
    variant: 'text',
})(({ theme }) => ({
    color: theme.colors.icon,
    padding: '0px',

    '&:hover': {
        border: 'none',
        backgroundColor: 'transparent',
        borderColor: 'transparent',
        boxShadow: shadows.none,
    },

    '&:focus': {
        border: 'none',
        backgroundColor: 'transparent',
        boxShadow: `0 0 0 2px ${theme.colors.bg}, 0 0 0 4px ${theme.colors.borderBrandFocused}`,
    },
}));

export default function SelectActionButtons({
    hasSelectedValues,
    isOpen,
    isDisabled,
    isReadOnly,
    showClear,
    fontSize = 'md',
    handleClearSelection,
}: ActionButtonsProps) {
    const onClearClickHandler = useCallback(
        (event: React.MouseEvent<HTMLButtonElement>) => {
            handleClearSelection?.();
            event.stopPropagation();
        },
        [handleClearSelection],
    );

    return (
        <ActionButtonsContainer>
            {showClear && hasSelectedValues && !isDisabled && !isReadOnly && (
                <StyledClearButton
                    icon={{ icon: 'X', source: 'phosphor', size: 'md' }}
                    isCircle
                    size={fontSize}
                    onClick={onClearClickHandler}
                    data-testid="button-clear"
                />
            )}
            <StyledIcon icon="CaretDown" source="phosphor" rotate={isOpen ? '180' : '0'} size="md" />
        </ActionButtonsContainer>
    );
}
