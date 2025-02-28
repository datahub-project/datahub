import { Button, Typography } from 'antd';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { REDESIGN_COLORS, SEARCH_COLORS } from '../../entityV2/shared/constants';

export const SearchFilterLabel = styled(Button)<{ $isActive: boolean }>`
    font-size: 14px;
    font-weight: 700;
    border: none;
    border-radius: 8px;
    display: flex;
    align-items: center;
    box-shadow: none;
    color: ${REDESIGN_COLORS.TEXT_HEADING};
    ${(props) =>
        props.$isActive &&
        `
        background-color: ${SEARCH_COLORS.TITLE_PURPLE};
        border: 1px solid ${SEARCH_COLORS.BACKGROUND_PURPLE};
        color: white;
    `}
`;

export const MoreFilterOptionLabel = styled.div<{ $isActive: boolean; isOpen?: boolean }>`
    padding: 5px 12px;
    font-size: 14px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    cursor: pointer;

    &:hover {
        background-color: ${ANTD_GRAY[3]};
    }

    ${(props) => props.$isActive && `color: ${SEARCH_COLORS.TITLE_PURPLE};`}
    ${(props) => props.isOpen && `background-color: ${ANTD_GRAY[3]};`}
`;

export const TextButton = styled(Button)<{ marginTop?: number; height?: number }>`
    color: ${SEARCH_COLORS.TITLE_PURPLE};
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
