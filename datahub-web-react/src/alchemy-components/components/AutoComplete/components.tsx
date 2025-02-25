import { colors, radius, transition } from '@src/alchemy-components/theme';
import styled from 'styled-components';

export const BOX_SHADOW = `0px -3px 12px 0px rgba(236, 240, 248, 0.5) inset,
0px 3px 12px 0px rgba(255, 255, 255, 0.5) inset,
0px 20px 60px 0px rgba(0, 0, 0, 0.12)`;

export const DropdownWrapper = styled.div`
    & .rc-virtual-list-scrollbar-thumb {
        background: rgba(193, 196, 208, 0.8) !important;
    }
    & .rc-virtual-list-scrollbar-show {
        background: rgba(193, 196, 208, 0.3) !important;
    }

    background: ${colors.white};
    border-radius: ${radius.lg};
    box-shadow: ${BOX_SHADOW};
    backdrop-filter: blur(20px);
`;

export const ChildrenWrapper = styled.div<{ $open?: boolean; $showWrapping?: boolean }>`
    background: transparent;

    ${(props) =>
        props.$showWrapping &&
        `
        padding: ${radius.md};
        transition: all ${transition.easing['ease-in']} ${transition.duration.slow};
        border-radius: ${radius.lg} ${radius.lg} ${radius.none} ${radius.none};
    `}

    ${(props) =>
        props.$open &&
        props.$showWrapping &&
        `
        background: ${colors.gray[1500]};
        box-shadow: ${BOX_SHADOW};
    `}
`;
