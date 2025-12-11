/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Sidebar } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';

import { useNavBarContext } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
import { colors } from '@src/alchemy-components';
import analytics, { EventType } from '@src/app/analytics';

const Toggler = styled.button<{ $isCollapsed?: boolean }>`
    cursor: pointer;
    margin: 0 0 0 auto;
    padding: 4px;
    border-radius: 6px;
    border: none;
    display: flex;
    transition: left 250ms ease-in-out;
    transition: background 300ms ease-in;
    background: ${colors.gray[1600]};

    &: hover {
        background: #ebecf080;
    }

    & svg {
        height: 20px;
        width: 20px;
        color: ${colors.gray[1800]};
    }
`;

export default function NavBarToggler() {
    const { toggle, isCollapsed } = useNavBarContext();

    function handleToggle() {
        analytics.event({ type: EventType.NavBarExpandCollapse, isExpanding: isCollapsed });
        toggle();
    }

    return (
        <Toggler onClick={handleToggle} aria-label="Navbar toggler">
            <Sidebar />
        </Toggler>
    );
}
