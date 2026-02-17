import { Sidebar } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';

import { useNavBarContext } from '@app/homeV2/layout/navBarRedesign/NavBarContext';
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
    background: ${(props) => props.theme.colors.bgSurfaceNewNav};

    &: hover {
        background: ${(props) => props.theme.colors.border}80;
    }

    & svg {
        height: 20px;
        width: 20px;
        color: ${(props) => props.theme.colors.textTertiary};
    }
`;

export default function NavBarToggler() {
    const { toggle, isCollapsed } = useNavBarContext();

    function handleToggle() {
        analytics.event({ type: EventType.NavBarExpandCollapse, isExpanding: isCollapsed });
        toggle();
    }

    return (
        <Toggler onClick={handleToggle} aria-label="Navbar toggler" data-testid="nav-bar-toggler">
            <Sidebar />
        </Toggler>
    );
}
