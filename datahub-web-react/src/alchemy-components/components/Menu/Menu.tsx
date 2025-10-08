import React from 'react';
import styled from 'styled-components';

import { Dropdown } from '@components/components/Dropdown';
import useConvertedAntdMenu from '@components/components/Menu/hooks/useConvertedAntdMenu';
import { MenuProps } from '@components/components/Menu/types';

const StyledDropdownContainer = styled.div`
    max-width: 335px;

    .ant-dropdown-menu {
        border-radius: 12px;
    }

    &&& {
        .ant-dropdown-menu-sub {
            border-radius: 12px;
        }
    }
`;

export function Menu({ children, items, ...props }: React.PropsWithChildren<MenuProps>) {
    const menu = useConvertedAntdMenu(items);

    return (
        <Dropdown
            {...props}
            dropdownRender={(originNode) => <StyledDropdownContainer>{originNode}</StyledDropdownContainer>}
            menu={menu}
            resetDefaultMenuStyles
        >
            {children}
        </Dropdown>
    );
}
