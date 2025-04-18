import styled from 'styled-components/macro';
import { Menu } from 'antd';

export const MenuItem = styled(Menu.Item)`
    display: flex;
    justify-content: start;
    align-items: center;
    && {
        margin-top: 2px;
    }
    & > a:visited,
    & > a:active,
    & > a:focus {
        clear: both;
        border: none;
        outline: 0;
    }
`;
