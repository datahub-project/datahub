import MenuItem from 'antd/lib/menu/MenuItem';
import styled from 'styled-components';

export const MenuItemStyle = styled(MenuItem)`
    &&&& {
        background-color: transparent;
        a {
            color: inherit;
        }
    }
`;
