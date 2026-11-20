import styled from 'styled-components';

export const RAIL_WIDTH = 64;
const TAB_SIZE = 52;
const TAB_CONTENT_SIZE = 48;
const ICON_SIZE = 20;

export const Rail = styled.div`
    width: ${RAIL_WIDTH}px;
    flex-shrink: 0;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 4px;
    padding: 4px 0;
    user-select: none;
    background-color: ${(props) => props.theme.colors.bg};
`;

export const HiddenSvg = styled.svg`
    position: absolute;
    width: 0;
    height: 0;
    overflow: hidden;
`;

export const TabButton = styled.button<{ $active: boolean; $gradientId: string }>`
    width: ${TAB_SIZE}px;
    height: ${TAB_SIZE}px;
    flex-shrink: 0;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 2px;
    padding: 0;
    margin: 0;
    border: none;
    border-radius: 8px;
    cursor: pointer;
    font-family: inherit;
    color: ${(props) => props.theme.colors.textTertiary};
    background: ${(props) => (props.$active ? props.theme.colors.bgSelectedSubtle : 'transparent')};
    box-shadow: ${(props) => (props.$active ? props.theme.colors.shadowFocusBrand : 'none')};

    &:hover:not(:disabled) {
        background: ${(props) => (props.$active ? props.theme.colors.bgSelectedSubtle : props.theme.colors.bgHover)};
        box-shadow: ${(props) => props.theme.colors.shadowFocus};
    }

    &:focus-visible {
        outline: none;
        box-shadow: ${(props) => props.theme.colors.shadowFocusBrand};
    }

    &:disabled {
        cursor: not-allowed;
        color: ${(props) => props.theme.colors.textDisabled};
    }

    svg {
        color: ${(props) => (props.$active ? props.theme.colors.iconSelected : props.theme.colors.icon)};
        ${(props) => props.$active && `fill: url(#${props.$gradientId}) ${props.theme.colors.iconSelected};`}
    }

    &:disabled svg {
        color: ${(props) => props.theme.colors.iconDisabled};
    }
`;

export const IconContainer = styled.span`
    display: flex;
    align-items: center;
    justify-content: center;
    width: ${ICON_SIZE}px;
    height: ${ICON_SIZE}px;
`;

export const Label = styled.span<{ $active: boolean }>`
    display: block;
    width: ${TAB_CONTENT_SIZE}px;
    font-size: 10px;
    line-height: 14px;
    font-weight: ${(props) => (props.$active ? 500 : 400)};
    text-align: center;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;

    ${(props) =>
        props.$active &&
        `
        color: transparent;
        background: ${props.theme.colors.brandGradientSelected};
        background-clip: text;
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    `}
`;
