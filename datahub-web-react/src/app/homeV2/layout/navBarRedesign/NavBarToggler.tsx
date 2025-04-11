import { Sidebar } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';
import { colors } from '@src/alchemy-components';
import { useNavBarContext } from './NavBarContext';

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
    const { toggle } = useNavBarContext();

    return (
        <Toggler onClick={toggle} aria-label="Navbar toggler">
            <Sidebar />
        </Toggler>
    );
}
