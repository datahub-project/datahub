import { RightOutlined } from '@ant-design/icons';
import { Button } from 'antd';
import styled from 'styled-components';
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

export const ViewContainer = styled.div`
    display: grid;
    grid-template-columns: 0.5fr 90px 20px;
    gap: 10px;
    cursor: pointer;
    align-items: center;
`;

export const ViewIcon = styled.div<{ selected?: boolean }>`
    border: 1px solid ${REDESIGN_COLORS.BORDER_1};
    display: flex;
    align-items: center;
    border-radius: 10px;
    padding: 20px;
    position: relative;
    border: ${(props) => (props.selected ? `1px solid ${ANTD_GRAY[1]} !important` : '')};
    background: ${(props) => (props.selected ? SEARCH_COLORS.TITLE_PURPLE : REDESIGN_COLORS.BORDER_1)};
    &.static {
        border: 1px solid ${ANTD_GRAY[1]};
    }
`;

export const ViewContent = styled.div`
    min-width: 100px;
`;

export const ViewLabel = styled.div`
    font-size: 14px;
    font-weight: 400;
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
`;

export const ViewDescription = styled.div`
    font-weight: 400;
    opacity: 0.5;
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
`;
