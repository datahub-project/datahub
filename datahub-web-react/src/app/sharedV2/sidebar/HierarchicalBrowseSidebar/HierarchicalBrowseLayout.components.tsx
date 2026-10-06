import styled from 'styled-components';

import {
    HIERARCHICAL_BROWSE_GAP_PX,
    HIERARCHICAL_BROWSE_LAYOUT_PADDING_PX,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';

export const HierarchicalBrowseContentWrapper = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    display: flex;
    flex: 1;
    min-height: 0;
    overflow: hidden;
    align-items: stretch;
    gap: ${(props) => (props.$isShowNavBarRedesign ? `${HIERARCHICAL_BROWSE_GAP_PX}px` : '0')};
    ${(props) => props.$isShowNavBarRedesign && `padding: ${HIERARCHICAL_BROWSE_LAYOUT_PADDING_PX}px;`}
`;

export const HierarchicalBrowseMainContent = styled.div`
    flex: 1;
    min-width: 0;
    min-height: 0;
    height: 100%;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    align-self: stretch;
`;
