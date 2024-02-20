import styled from 'styled-components';
import { Button } from 'antd';
import { ANTD_GRAY, REDESIGN_COLORS, SEARCH_COLORS } from '../constants';

const MenuItem = styled.div`
    font-size: 12px;
    padding: 0 4px;
    color: #262626;
`;

export const ActionMenuItem = styled(Button)<{ disabled?: boolean }>`
    border-radius: 20px;
    width: 28px;
    height: 28px;
    margin: 0px 4px;
    padding: 0px;
    display: flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
    border: none;
    background-color: #f7f7f7;
    border: 1px solid #eee;
    color: ${REDESIGN_COLORS.ACTION_ICON_GREY};
    box-shadow: none;
    &&:hover {
        background-color: ${ANTD_GRAY[3]};
        color: ${SEARCH_COLORS.TITLE_PURPLE};
        border-color: ${SEARCH_COLORS.TITLE_PURPLE};
    }
    ${(props) =>
        props.disabled
            ? `
            ${MenuItem} {
                color: ${ANTD_GRAY[7]};
            }
    `
            : ''};
`;
