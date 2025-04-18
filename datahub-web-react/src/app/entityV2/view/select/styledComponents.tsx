import { RightOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import styled from 'styled-components';
import { colors } from '@src/alchemy-components';
import { ANTD_GRAY, REDESIGN_COLORS, SEARCH_COLORS } from '../../shared/constants';

export const NoMarginButton = styled(Button)`
    && {
        margin: 0px;
    }
`;

export const StyledRightOutlined = styled(RightOutlined)`
    && {
        font-size: 8px;
        color: ${ANTD_GRAY[7]};
    }
`;

export const ViewContainer = styled.div<{ $selected?: boolean; $isShowNavBarRedesign?: boolean }>`
    ${(props) =>
        !props.$isShowNavBarRedesign &&
        `
        display: grid;
        grid-template-columns: 0.5fr 90px 20px;
        gap: 10px;
    `}
    cursor: pointer;
    align-items: center;

    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        padding: 8px;
        border-radius: 8px;
        display: flex;
        background-color: white;
        gap: 8px;
        width: 100%;
        width: 260px;
        height: 64px;
        border: 1px solid ${props.$selected ? colors.violet[500] : colors.gray[100]};

        :hover {
            border: 1px solid ${colors.violet[500]};
            box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07);
        }
    `}
`;

export const ViewIcon = styled.div<{ $selected?: boolean }>`
    border: 1px solid ${REDESIGN_COLORS.BORDER_1};
    display: flex;
    align-items: center;
    border-radius: 10px;
    padding: 20px;
    position: relative;
    border: ${(props) => (props.$selected ? `1px solid ${ANTD_GRAY[1]} !important` : '')};
    background: ${(props) => (props.$selected ? SEARCH_COLORS.TITLE_PURPLE : REDESIGN_COLORS.BORDER_1)};
    &.static {
        border: 1px solid ${ANTD_GRAY[1]};
    }
`;

export const ViewIconNavBarRedesign = styled.div<{ $selected?: boolean }>`
    background-color: ${(props) => (props.$selected ? colors.gray[1000] : colors.gray[1500])};
    border-radius: 200px;
    height: 32px;
    width: 32px;
    padding: 0 6px 0 4px;
    display: flex;
    align-items: center;
    justify-content: center;

    svg {
        color: ${(props) => (props.$selected ? '#705EE4' : colors.gray[1800])};
    }
`;

export const ViewContent = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    ${(props) => !props.$isShowNavBarRedesign && 'min-width: 100px;'}
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        color: black;
        min-width: 160px;
    `}
`;

export const ViewLabel = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    ${(props) => props.$isShowNavBarRedesign && `color: ${colors.gray[600]};`}
    font-size: 14px;
    font-weight: 400;
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
    line-height: 18px;
`;

export const ViewDescription = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    font-weight: 400;
    ${(props) => !props.$isShowNavBarRedesign && 'opacity: 0.5;'}
    ${(props) => props.$isShowNavBarRedesign && `color: ${colors.gray[1700]};`}
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
`;
