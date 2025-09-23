import React, { useCallback } from 'react';
import styled from 'styled-components';

import { Button } from '@components/components/Button';
import { ActionButtonsContainer, StyledIcon } from '@components/components/Select/components';
import { ActionButtonsProps } from '@components/components/Select/types';

import { colors, shadows } from '@src/alchemy-components/theme';

export const StyledClearButton = styled(Button).attrs({
    variant: 'text',
})({
    color: colors.gray[1800],
    padding: '0px',

    '&:hover': {
        border: 'none',
        backgroundColor: colors.transparent,
        borderColor: colors.transparent,
        boxShadow: shadows.none,
    },

    '&:focus': {
        border: 'none',
        backgroundColor: colors.transparent,
        boxShadow: `0 0 0 2px ${colors.white}, 0 0 0 4px ${colors.violet[50]}`,
    },
});

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
                />
            )}
            <StyledIcon icon="CaretDown" source="phosphor" rotate={isOpen ? '180' : '0'} size="md" color="gray" />
        </ActionButtonsContainer>
    );
}
