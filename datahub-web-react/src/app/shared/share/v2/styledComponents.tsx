import { Menu } from 'antd';
import styled from 'styled-components';

export const StyledMenuItem = styled(Menu.Item)`
    min-width: 120px;
    background-color: ${(props) => props.theme.colors.bg} !important;
    height: 28px;
    font-size: 12px;
    font-weight: 300;
    padding: 12px;
    color: ${(props) => props.theme.colors.textSecondary};
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
