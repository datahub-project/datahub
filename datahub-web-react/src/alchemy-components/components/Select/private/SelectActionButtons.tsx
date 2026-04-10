import { CaretDown } from '@phosphor-icons/react/dist/csr/CaretDown';
import React, { useCallback } from 'react';

import { ActionButtonsContainer, ClearButton, StyledIcon } from '@components/components/Select/components';
import { ActionButtonsProps } from '@components/components/Select/types';

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
                <ClearButton size={fontSize} onClick={onClearClickHandler} data-testid="button-clear" />
            )}
            <StyledIcon
                icon={CaretDown}
                rotate={isOpen ? '180' : '0'}
                size="md"
                color="gray"
                data-testid="button-expand"
            />
        </ActionButtonsContainer>
    );
}
