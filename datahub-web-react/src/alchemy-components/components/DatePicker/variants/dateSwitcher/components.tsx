import { colors } from '@components';
import { CaretLeft, CaretRight } from 'phosphor-react';
import React, { useCallback, useMemo } from 'react';
import styled from 'styled-components';
import { Text } from '../../../Text/Text';
import { SwitcherDirection } from './types';
import { ExtendedInputRenderProps } from '../../types';

const StyledContainer = styled.div<{ $opened?: boolean; $disabled?: boolean }>`
    border: 1px solid ${(props) => (props.$opened || props.$disabled ? colors.gray[1800] : colors.gray[100])};
    ${(props) => props.$opened && !props.$disabled && `outline: 2px solid ${colors.violet[300]};`}
    border-radius: 8px;
    padding: 8px;
    display: flex;
    flex-direction: row;
    gap: 8px;
    justify-content: space-between;
    align-items: center;
    width: 100%;

    box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);

    ${(props) =>
        props.$disabled &&
        `
        background: ${colors.gray[1500]};
        cursor: not-allowed;
    `}

    :hover,
    :focus,
    :active {
        ${(props) => !props.$disabled && 'box-shadow: 0px 1px 2px 1px rgba(33, 23, 95, 0.07);'}
    }
`;

const Content = styled(Text)<{ $disabled?: boolean }>`
    color: ${colors.gray[1800]};
    user-select: none;
    cursor: ${(props) => (props.$disabled ? 'not-allowed' : 'pointer')};

    :hover {
        ${(props) => !props.$disabled && `color: ${colors.violet[500]};`}
    }
`;

const CaretWrapper = styled.div<{ $disabled?: boolean }>`
    & svg {
        color: ${colors.gray[1800]};
        display: flex;
        align-items: center;
        cursor: ${(props) => (props.$disabled ? 'not-allowed' : 'pointer')};

        :hover {
            ${(props) => !props.$disabled && `color: ${colors.violet[500]};`}
        }
    }
`;

type SwitcherButtonProps = {
    direction: SwitcherDirection;
    onClick: (direction: SwitcherDirection) => void;
    disabled?: boolean;
};

function SwitcherButton({ direction, onClick, disabled }: SwitcherButtonProps) {
    const Icon = direction === SwitcherDirection.Left ? CaretLeft : CaretRight;

    const onClickHandler = useCallback(() => {
        if (disabled) return null;
        return onClick?.(direction);
    }, [direction, disabled, onClick]);

    return (
        <CaretWrapper $disabled={disabled} onClick={onClickHandler} tabIndex={0}>
            <Icon size="20px" />
        </CaretWrapper>
    );
}

export function DateSwitcherInput({ datePickerProps, datePickerState, ...props }: ExtendedInputRenderProps) {
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

    return (
        <StyledContainer $opened={open} $disabled={disabled} tabIndex={0}>
            <SwitcherButton
                disabled={isDateSwitchingDisabled}
                direction={SwitcherDirection.Left}
                onClick={onSwitcherClick}
            />

            <Content $disabled={disabled} onMouseDown={props.onMouseDown} onKeyDown={props.onKeyDown}>
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
