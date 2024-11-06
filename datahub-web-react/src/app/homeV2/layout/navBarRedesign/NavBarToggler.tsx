import { Sidebar } from '@phosphor-icons/react';
import React from 'react';
import styled from 'styled-components';
import { REDESIGN_COLORS } from '@src/app/entityV2/shared/constants';
import { useNavBarContext } from './NavBarContext';

const Toggler = styled.button<{ $isCollapsed?: boolean; $iconSize: number }>`
    cursor: pointer;
    margin: 0 0 0 auto;
    padding: 8px;
    border-radius: 6px;
    border: none;
    display: flex;
    transition: left 250ms ease-in-out;
    transition: background 300ms ease-in;
    background: ${REDESIGN_COLORS.BACKGROUUND_NAVBAR_REDESIGN};

    &: hover {
        background: #ebecf080;
    }

    & svg {
        height: ${(props) => `${props.$iconSize}px`};
        width: ${(props) => `${props.$iconSize}px`};
        color: ${REDESIGN_COLORS.GREY_300};
    }
`;

type Props = {
    iconSize: number;
};

export default function NavBarToggler({ iconSize }: Props) {
    const { toggle } = useNavBarContext();

    return (
        <Toggler onClick={toggle} $iconSize={iconSize}>
            <Sidebar />
        </Toggler>
    );
}
