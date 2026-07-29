import { css } from 'styled-components';

/** Shared thin scrollbar used by TreeContainer and CollapsedScrollColumn. */
export const hierarchicalBrowseScrollbar = css`
    scrollbar-gutter: stable;

    &::-webkit-scrollbar {
        width: 6px;
    }

    &::-webkit-scrollbar-track {
        background: ${(props) => props.theme.colors.scrollbarTrack};
    }

    &::-webkit-scrollbar-thumb {
        background: ${(props) => props.theme.colors.scrollbarThumb};
        border-radius: 3px;
    }

    &::-webkit-scrollbar-thumb:hover {
        background: ${(props) => props.theme.colors.scrollbarThumbHover};
    }

    scrollbar-width: thin;
    scrollbar-color: ${(props) => `${props.theme.colors.scrollbarThumb} ${props.theme.colors.scrollbarTrack}`};
`;
