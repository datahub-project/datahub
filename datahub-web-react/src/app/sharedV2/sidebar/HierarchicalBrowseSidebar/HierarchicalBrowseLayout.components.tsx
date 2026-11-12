import styled from 'styled-components';

import { PAGE_GAP_PX } from '@app/sharedV2/layouts/constants';

/** Sidebar + page row. No outer padding: the app shell already insets every page. */
export const HierarchicalBrowseContentWrapper = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    display: flex;
    flex: 1;
    min-height: 0;
    overflow: hidden;
    align-items: stretch;
    gap: ${(props) => (props.$isShowNavBarRedesign ? `${PAGE_GAP_PX}px` : '0')};
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
