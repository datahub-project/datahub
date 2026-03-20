import styled from 'styled-components';

export const SearchFilterLabel = styled.div<{ $isActive: boolean }>`
    font-size: 14px;
    font-weight: 400;
    line-height: none;
    border: 1px solid ${(props) => (props.$isActive ? props.theme.colors.borderSelected : props.theme.colors.border)};
    border-radius: 8px;
    display: flex;
    align-items: center;
    gap: 4px;
    padding: 4px 8px;
    min-height: 36px;
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
    color: ${(props) => (props.$isActive ? props.theme.colors.textSelected : props.theme.colors.text)};
    background-color: ${(props) => (props.$isActive ? props.theme.colors.bgSelected : props.theme.colors.bg)};
    box-shadow: ${(props) => props.theme.colors.shadowXs};
    transition:
        box-shadow 0.15s ease,
        border-color 0.15s ease;

    svg {
        color: ${(props) => props.theme.colors.icon};
    }

    &:hover {
        box-shadow: ${(props) => props.theme.colors.shadowSm};
    }
`;

export const MoreFilterOptionLabel = styled.div<{ $isActive: boolean; isOpen?: boolean }>`
    padding: 5px 12px;
    font-size: 14px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    cursor: pointer;
    border-radius: 8px;
    max-width: 100%;
    color: ${(props) => (props.$isActive ? props.theme.colors.textBrand : props.theme.colors.text)};
    background-color: ${(props) => (props.isOpen ? props.theme.colors.bgHover : 'transparent')};

    svg {
        color: ${(props) => props.theme.colors.icon};
        flex-shrink: 0;
    }

    &:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
    }
`;

export const TextButton = styled.button<{ marginTop?: number; height?: number }>`
    color: ${(props) => props.theme.colors.textBrand};
    background: none;
    border: none;
    padding: 0px 6px;
    cursor: pointer;
    font-size: 14px;
    margin-top: ${(props) => (props.marginTop !== undefined ? `${props.marginTop}px` : '8px')};
    ${(props) => props.height !== undefined && `height: ${props.height}px;`}

    &:hover {
        background-color: ${(props) => props.theme.colors.bgHover};
        border-radius: 4px;
    }
`;

export const Label = styled.span`
    max-width: 125px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    display: inline-block;
    color: inherit;
`;
