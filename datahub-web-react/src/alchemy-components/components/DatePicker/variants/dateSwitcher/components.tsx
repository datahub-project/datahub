import { Button } from '@components';
import { CaretLeft } from '@phosphor-icons/react/dist/csr/CaretLeft';
import { CaretRight } from '@phosphor-icons/react/dist/csr/CaretRight';
import React, { useCallback, useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { ExtendedInputRenderProps } from '@components/components/DatePicker/types';
import { SwitcherDirection } from '@components/components/DatePicker/variants/dateSwitcher/types';
import { Text } from '@components/components/Text/Text';

const StyledContainer = styled.div<{ $opened?: boolean; $disabled?: boolean }>`
    border: 1px solid
        ${(props) => {
            if (props.$disabled) return props.theme.colors.borderDisabled;
            if (props.$opened) return props.theme.colors.borderBrandFocused;
            return props.theme.colors.border;
        }};
    ${(props) => props.$opened && !props.$disabled && `outline: 1px solid ${props.theme.colors.borderBrandFocused};`}
    border-radius: 8px;
    padding: 8px;
    display: flex;
    flex-direction: row;
    gap: 8px;
    justify-content: space-between;
    align-items: center;
    width: 100%;
    background: ${(props) => props.theme.colors.bg};

    box-shadow: ${(props) => props.theme.colors.shadowXs};

    ${(props) =>
        props.$disabled &&
        `
        background: ${props.theme.colors.bgInputDisabled};
        cursor: not-allowed;
    `}

    :hover,
    :focus-within,
    :active {
        ${(props) => !props.$disabled && `box-shadow: ${props.theme.colors.shadowSm};`}
    }
`;

const Content = styled(Text).attrs({ type: 'div' as const })<{ $disabled?: boolean }>`
    color: ${(props) => props.theme.colors.textTertiary};
    user-select: none;
    cursor: ${(props) => (props.$disabled ? 'not-allowed' : 'pointer')};
    flex: 1;
    text-align: center;

    :hover {
        ${(props) => !props.$disabled && `color: ${props.theme.colors.textHover};`}
    }

    &:focus-visible {
        outline: 2px solid ${(props) => props.theme.colors.borderBrandFocused};
        outline-offset: 2px;
    }
`;

const CaretWrapper = styled(Button)`
    padding: 0;
    min-width: unset;

    & svg {
        color: ${(props) => props.theme.colors.textTertiary};
        display: flex;
        align-items: start;
    }

    &:hover:not(:disabled) svg,
    &:focus-visible:not(:disabled) svg {
        color: ${(props) => props.theme.colors.textHover};
    }
`;

type SwitcherButtonProps = {
    direction: SwitcherDirection;
    onClick: (direction: SwitcherDirection) => void;
    disabled?: boolean;
};

function SwitcherButton({ direction, onClick, disabled }: SwitcherButtonProps) {
    const Icon = direction === SwitcherDirection.Left ? CaretLeft : CaretRight;
    const { t } = useTranslation('alchemy');
    const ariaLabel = direction === SwitcherDirection.Left ? t('datePicker.previousDay') : t('datePicker.nextDay');

    const onClickHandler = useCallback(() => {
        if (disabled) return null;
        return onClick?.(direction);
    }, [direction, disabled, onClick]);

    return (
        <CaretWrapper
            type="button"
            variant="text"
            color="gray"
            disabled={disabled}
            onClick={onClickHandler}
            aria-label={ariaLabel}
        >
            <Icon size="20px" />
        </CaretWrapper>
    );
}

export function DateSwitcherInput({ datePickerProps, datePickerState, ...props }: ExtendedInputRenderProps) {
    const { t } = useTranslation('alchemy');
    const { disabled } = datePickerProps;
    const { setValue, open } = datePickerState;

    const onSwitcherClick = useCallback(
        (direction: SwitcherDirection) => {
            if (disabled) return null;

            return setValue?.((currentValue) => {
                if (!currentValue) return currentValue;
                // FYI: clone value to trigger rerendering after changes
                const cloneOfCurrentValue = currentValue.clone();
                const sign = direction === SwitcherDirection.Left ? -1 : 1;
                const newValue = cloneOfCurrentValue.add(1 * sign, 'day');
                return newValue;
            });
        },
        [setValue, disabled],
    );

    const isDateSwitchingDisabled = useMemo(() => disabled || !props.title, [disabled, props.title]);
    const ariaLabel =
        typeof props.title === 'string' && props.title ? props.title : props.placeholder || t('datePicker.placeholder');

    return (
        <StyledContainer $opened={open} $disabled={disabled}>
            <SwitcherButton
                disabled={isDateSwitchingDisabled}
                direction={SwitcherDirection.Left}
                onClick={onSwitcherClick}
            />

            <Content
                $disabled={disabled}
                role="button"
                tabIndex={disabled ? -1 : 0}
                aria-label={ariaLabel}
                aria-disabled={disabled}
                onMouseDown={props.onMouseDown}
                onKeyDown={props.onKeyDown}
            >
                {props.title ? props.title : props.placeholder}
            </Content>

            <SwitcherButton
                disabled={isDateSwitchingDisabled}
                direction={SwitcherDirection.Right}
                onClick={onSwitcherClick}
            />
        </StyledContainer>
    );
}
