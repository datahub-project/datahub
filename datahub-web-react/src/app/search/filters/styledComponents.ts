import { Button, Typography } from 'antd';
import styled from 'styled-components';

export const SearchFilterLabel = styled(Button)<{ $isActive: boolean }>`
    font-size: 14px;
    font-weight: 700;
    margin-right: 12px;
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 8px;
    display: flex;
    align-items: center;
    box-shadow: none;
    ${(props) =>
        props.$isActive &&
        `
        background-color: ${props.theme.colors.buttonFillBrand};
        border: 1px solid ${props.theme.colors.borderBrand};
        color: ${props.theme.colors.textOnFillBrand};
    `}
`;

export const MoreFilterOptionLabel = styled.div<{ $isActive: boolean; isOpen: boolean }>`
    padding: 5px 12px;
    font-size: 14px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    cursor: pointer;

    &:hover {
        background-color: ${(props) => props.theme.colors.bgSurface};
    }

    ${(props) => props.$isActive && `color: ${props.theme.colors.textBrand};`}
    ${(props) => props.isOpen && `background-color: ${props.theme.colors.bgSurface};`}
`;

export const Label = styled(Typography.Text)`
    max-width: 125px;
`;

export const IconSpacer = styled.span`
    width: 4px;
`;
