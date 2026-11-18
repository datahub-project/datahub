import styled from 'styled-components';

export const DropdownWrapper = styled.div`
    & .rc-virtual-list-scrollbar-thumb {
        background: ${({ theme }) => theme.colors.scrollbarThumb} !important;
    }
    & .rc-virtual-list-scrollbar-show {
        background: ${({ theme }) => theme.colors.scrollbarTrack} !important;
    }
`;

// Holds the capture-phase key handler without adding a box around the select
export const KeyCaptureWrapper = styled.div`
    display: contents;
`;
