/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
