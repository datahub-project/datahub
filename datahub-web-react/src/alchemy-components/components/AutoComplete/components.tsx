import styled from 'styled-components';

export const DropdownWrapper = styled.div`
    & .rc-virtual-list-scrollbar-thumb {
        background: ${({ theme }) => theme.colors.scrollbarThumb} !important;
    }
    & .rc-virtual-list-scrollbar-show {
        background: ${({ theme }) => theme.colors.scrollbarTrack} !important;
    }
`;
