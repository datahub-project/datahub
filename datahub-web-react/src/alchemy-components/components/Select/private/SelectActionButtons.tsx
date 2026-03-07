import React, { useCallback } from 'react';
import styled from 'styled-components';

import { Button } from '@components/components/Button';
import { ActionButtonsContainer, StyledIcon } from '@components/components/Select/components';
import { ActionButtonsProps } from '@components/components/Select/types';

export const StyledClearButton = styled(Button).attrs({
    variant: 'text',
})`
    color: ${(props) => props.theme.colors.text};
    padding: 0px;

    &:hover {
        border: none;
        background-color: transparent;
        border-color: transparent;
        box-shadow: none;
    }

    &:focus {
        border: none;
        background-color: transparent;
        box-shadow:
            0 0 0 2px ${(props) => props.theme.colors.bg},
            0 0 0 4px ${(props) => props.theme.colors.bgSurfaceBrand};
    }
`;

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
                    icon={{ icon: 'X', size: 'md' }}
                    isCircle
                    size={fontSize}
                    onClick={onClearClickHandler}
                    data-testid="button-clear"
                />
            )}
            <StyledIcon icon="CaretDown" rotate={isOpen ? '180' : '0'} size="md" color="gray" />
        </ActionButtonsContainer>
    );
}
