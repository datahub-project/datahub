import { Button, Typography } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../entity/shared/constants';

export const SearchFilterLabel = styled(Button)<{ isActive: boolean }>`
    font-size: 14px;
    font-weight: 700;
    margin-right: 12px;
    border: 1px solid ${ANTD_GRAY[5]};
    border-radius: 8px;
    display: flex;
    align-items: center;
    box-shadow: none;
    ${(props) =>
        props.isActive &&
        `
        background-color: ${props.theme.styles['primary-color']};
        border: 1px solid ${props.theme.styles['primary-color']};
        color: white;
    `}
`;

export const MoreFilterOptionLabel = styled.div<{ isActive: boolean; isOpen: boolean }>`
    padding: 5px 12px;
    font-size: 14px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    cursor: pointer;

    &:hover {
        background-color: ${ANTD_GRAY[3]};
    }

    ${(props) => props.isActive && `color: ${props.theme.styles['primary-color']};`}
    ${(props) => props.isOpen && `background-color: ${ANTD_GRAY[3]};`}
`;

export const TextButton = styled(Button)<{ marginTop?: number; height?: number }>`
    color: ${(props) => props.theme.styles['primary-color']};
    padding: 0px 6px;
    margin-top: ${(props) => (props.marginTop !== undefined ? `${props.marginTop}px` : '8px')};
    ${(props) => props.height !== undefined && `height: ${props.height}px;`}

    &:hover {
        background-color: white;
    }
`;

export const Label = styled(Typography.Text)`
    max-width: 125px;
`;

export const IconSpacer = styled.span`
    width: 4px;
`;
