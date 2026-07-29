import styled from 'styled-components';

import {
    HIERARCHICAL_BROWSE_GAP_PX,
    HIERARCHICAL_BROWSE_LAYOUT_PADDING_PX,
} from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';

/**
 * Shared route layout for hierarchical browse pages (Glossary, Domains,
 * Documents, Metrics). Keeps sidebar ↔ page gap and height alignment identical.
 *
 * Usage:
 * ```tsx
 * <HierarchicalBrowseContentWrapper $isShowNavBarRedesign={...}>
 *   <SomeSidebar />
 *   <HierarchicalBrowseMainContent>
 *     <Switch>...</Switch>
 *   </HierarchicalBrowseMainContent>
 * </HierarchicalBrowseContentWrapper>
 * ```
 */
export const HierarchicalBrowseContentWrapper = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    display: flex;
    flex: 1;
    min-height: 0;
    overflow: hidden;
    align-items: stretch;
    gap: ${(props) => (props.$isShowNavBarRedesign ? `${HIERARCHICAL_BROWSE_GAP_PX}px` : '0')};
    ${(props) => props.$isShowNavBarRedesign && `padding: ${HIERARCHICAL_BROWSE_LAYOUT_PADDING_PX}px;`}
`;

/** Right-hand page column — stretches to the same height as the sidebar. */
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
