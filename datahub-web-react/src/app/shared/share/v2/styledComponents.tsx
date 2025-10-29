import { Button, Checkbox, Form, Menu, Modal, Typography } from 'antd';
import styled from 'styled-components';

import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';

export const StyledMenuItem = styled(Menu.Item)`
    min-width: 120px;
    background-color: #fff !important;
    height: 28px;
    font-size: 12px;
    font-weight: 300;
    padding: 12px;
    color: #46507b;
    :hover {
        background-color: transparent;
    }
`;

export const StyledMenu = styled(Menu)`
    border-radius: 4px;
    min-width: 140px;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 5px;
    border-right: none;
`;
