import { Button } from 'antd';
import styled from 'styled-components';

import { ANTD_GRAY, REDESIGN_COLORS } from '@app/entityV2/shared/constants';

const MenuItem = styled.div`
    font-size: 12px;
    padding: 0 4px;
    color: #262626;
`;

export const ActionMenuItem = styled(Button)<{ disabled?: boolean; fontSize?: number; excludeMargin?: boolean }>`
    border-radius: 20px;
    width: ${(props) => (props.fontSize ? `${props.fontSize}px` : '28px')};
    height: ${(props) => (props.fontSize ? `${props.fontSize}px` : '28px')};
    margin: 0px ${(props) => (props.excludeMargin ? '0px' : '4px')};
    padding: 0px;
    display: flex;
    align-items: center;
    justify-content: center;
    overflow: hidden;
    border: none;
    background-color: 'white';
    border: 1px solid #eee;
    color: ${REDESIGN_COLORS.ACTION_ICON_GREY};
    box-shadow: none;
    &&:hover {
        background-color: ${ANTD_GRAY[3]};
        color: ${(props) => props.theme.styles['primary-color']};
        border-color: ${(props) => props.theme.styles['primary-color']};
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
