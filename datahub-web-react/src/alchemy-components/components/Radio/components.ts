import styled from 'styled-components';

import { getRadioBorderColor, getRadioCheckmarkColor } from '@components/components/Radio/utils';
import { borders, radius, spacing, typography } from '@components/theme';

export const RadioWrapper = styled.div<{ disabled: boolean }>`
    display: flex;
    align-items: center;
    gap: ${spacing.xsm};
    cursor: ${(props) => (props.disabled ? 'default' : 'pointer')};
`;

// Owns the circle. Also the positioning ancestor for the absolutely-placed Checkmark,
// so it must stay `relative`.
export const RadioBase = styled.div<{ disabled: boolean; error: string }>`
    position: relative;
    flex-shrink: 0;
    width: 20px;
    height: 20px;
    border: ${borders['2px']} ${(props) => getRadioBorderColor(props.disabled, props.error, props.theme.colors)};
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: ${radius.full};
    transition:
        border 0.3s ease,
        outline 0.3s ease;

    &:hover {
        border: ${borders['2px']}
            ${(props) =>
                !props.disabled && !props.error
                    ? props.theme.colors.borderBrand
                    : getRadioBorderColor(props.disabled, props.error, props.theme.colors)};
        outline: ${(props) =>
            !props.disabled && !props.error ? `${borders['2px']} ${props.theme.colors.border}` : 'none'};
    }
`;

export const Label = styled.label`
    color: ${(props) => props.theme.colors.text};
    display: flex;
    align-items: center;
    cursor: inherit;
    font-family: ${typography.fonts.body};
    font-size: ${typography.fontSizes.sm};
    font-weight: ${typography.fontWeights.bold};
`;

export const RadioLabel = styled.div`
    display: flex;
    align-items: center;
    min-width: 0;
`;

export const Required = styled.span`
    color: ${(props) => props.theme.colors.textError};
    margin-left: ${spacing.xxsm};
`;

export const Checkmark = styled.div<{ checked: boolean; disabled: boolean; error: string }>`
    width: calc(100% - 6px);
    height: calc(100% - 6px);
    border-radius: ${radius.full};
    background: ${(props) => getRadioCheckmarkColor(props.checked, props.disabled, props.error, props.theme.colors)};
    display: ${(props) => (props.checked ? 'inline-block' : 'none')};
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
`;

// Stretched over the circle rather than sized independently, so the whole circle is a hit target.
export const HiddenInput = styled.input<{ checked: boolean }>`
    opacity: 0;
    position: absolute;
    inset: 0;
    width: 100%;
    height: 100%;
    margin: 0;
    cursor: inherit;
`;

export const RadioGroupContainer = styled.div<{ isVertical?: boolean }>`
    display: flex;
    flex-direction: ${(props) => (props.isVertical ? 'column' : 'row')};
    align-items: ${(props) => (props.isVertical ? 'flex-start' : 'center')};
    gap: ${(props) => (props.isVertical ? spacing.xsm : spacing.md)};
`;
