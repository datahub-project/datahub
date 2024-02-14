import styled from 'styled-components';
import { Button } from 'antd';
import { ANTD_GRAY } from '../constants';

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
    background-color: #ffffff;
    border: 1px solid #f0f0f0;
    color: #5280e8;
    box-shadow: none;
    &&:hover {
        background-color: ${ANTD_GRAY[3]};
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
