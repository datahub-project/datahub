import { Sidebar } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';
import { colors } from '@src/alchemy-components';
import { useNavBarContext } from './NavBarContext';

const Toggler = styled.button<{ $isCollapsed?: boolean }>`
    cursor: pointer;
    margin: 0 0 0 auto;
    padding: 8px;
    border-radius: 6px;
    border: none;
    display: flex;
    transition: left 250ms ease-in-out;
    transition: background 300ms ease-in;
    background: ${colors.gray[1500]};

    &: hover {
        background: #ebecf080;
    }

    & svg {
        height: 20px;
        width: 20px;
        color: ${colors.gray[1700]};
    }
`;

export default function NavBarToggler() {
    const { toggle } = useNavBarContext();

    return (
        <Toggler onClick={toggle}>
            <Sidebar />
        </Toggler>
    );
}
