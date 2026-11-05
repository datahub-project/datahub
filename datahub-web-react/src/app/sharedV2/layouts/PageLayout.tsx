import { PageTitle } from '@components';
import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import { PAGE_GAP_PX } from '@app/sharedV2/layouts/constants';
import SidebarResizer from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/SidebarResizer';
import usePersistedSidebarWidth, { SIDE_PANEL_MAX_ROW_SHARE } from '@app/sharedV2/sidebar/usePersistedSidebarWidth';

const Card = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
    display: flex;
    flex-direction: column;
    overflow: hidden;
    border-radius: ${(props) => props.theme.styles['border-radius-navbar-redesign']};
    box-shadow: ${(props) => props.theme.colors.shadowNavbar};
`;

const PageWrapper = styled(Card)`
    display: flex;
    flex: 1;
    min-width: 0;
    min-height: 0;
    height: 100%;
    overflow: hidden;
`;

const PageTitleWrapper = styled.div`
    padding: 16px 20px;
`;

const ContentWrapper = styled.div`
    flex: 1;
    min-height: 0;
    overflow-y: auto;
`;

const Panel = styled(Card)`
    padding: 16px;
`;

const SidePanel = styled(Panel)<{
    $closed?: boolean;
    $width?: number;
    $noTransition?: boolean;
    $isResizable?: boolean;
}>`
    ${({ $isResizable }) => ($isResizable ? 'position: relative;' : '')}
    width: ${({ $closed, $width }) => {
        if ($closed) return '0px';
        // A saved drag width must not crowd out the page on a narrower window.
        if ($width) return `min(${$width}px, ${SIDE_PANEL_MAX_ROW_SHARE * 100}%)`;
        return '33.333%';
    }};
    flex-shrink: 0;
    /* Flex items default to min-width auto, so wide unwrappable chat content
       (SQL blocks, long entity links) would inflate the panel past its 33%
       and push it off-viewport, clipping the text. Clamp it: inner content
       scrolls instead. */
    min-width: 0;
    min-height: 0;
    max-width: ${({ $closed, $width }) => {
        if ($closed) return '0px';
        if ($width) return `min(${$width}px, ${SIDE_PANEL_MAX_ROW_SHARE * 100}%)`;
        return '33.333%';
    }};
    overflow: hidden;
    height: 100%;
    padding: 0;
    opacity: ${({ $closed }) => ($closed ? 0 : 1)};
    ${({ $closed, $noTransition }) => {
        if ($noTransition) return 'transition: opacity 0.2s ease;';
        return $closed
            ? 'box-shadow: none; transition: width 0.4s ease-in-out, opacity 0.2s ease;'
            : 'transition: width 0.4s ease-in-out, opacity 0.8s ease;';
    }}
`;

/** Bounds for the drag-resizable right panel. */
const MIN_RIGHT_PANEL_WIDTH = 320;
const MAX_RIGHT_PANEL_WIDTH = 800;
const DEFAULT_RIGHT_PANEL_RATIO = 0.3;

const BottomPanel = styled(Panel)`
    height: 62px;
`;

const VerticalContainer = styled.div`
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: ${PAGE_GAP_PX}px;
    /* Without this, one wide unbreakable descendant (a code block in the chat
       rail, a long link) inflates this column's min-content past the app row
       and the whole layout lays out wider than the viewport. */
    min-width: 0;
    min-height: 0;
    overflow: hidden;
`;

const HorizontalContainer = styled.div<{ $isRightPanelCollapsed?: boolean }>`
    flex: 1;
    display: flex;
    flex-direction: row;
    gap: ${PAGE_GAP_PX}px;
    min-width: 0;
    min-height: 0;
    overflow: hidden;
    /* Sized by the flex column inside the app shell, so the panels end on the shell's inset. */
    ${(props) =>
        props.$isRightPanelCollapsed &&
        `
            margin-right: -${PAGE_GAP_PX}px;
        `}
`;

const TopContainer = styled.div`
    padding: 16px 20px 0 20px;
    display: flex;
    justify-content: space-between;
`;

const TopRightContentContainer = styled.div`
    flex: 1;
    display: flex;
    justify-content: end;
`;

interface Props {
    title?: string;
    titlePill?: React.ReactNode;
    subTitle?: string | React.ReactNode;
    leftPanelContent?: React.ReactNode;
    rightPanelContent?: React.ReactNode;
    bottomPanelContent?: React.ReactNode;
    topBreadcrumb?: React.ReactNode;
    isRightPanelCollapsed?: boolean;
    topRightContent?: React.ReactNode;
    /** Allow the user to drag-resize the right panel. Off by default. */
    isRightPanelResizable?: boolean;
    /** When set, the resized right panel width is saved under this key (bounds and storage via `usePersistedSidebarWidth`). */
    rightPanelWidthLocalStorageKey?: string;
}

export function PageLayout({
    children,
    title,
    titlePill,
    subTitle,
    leftPanelContent,
    rightPanelContent,
    bottomPanelContent,
    topBreadcrumb,
    isRightPanelCollapsed,
    topRightContent,
    isRightPanelResizable,
    rightPanelWidthLocalStorageKey,
}: React.PropsWithChildren<Props>) {
    const { width: rightPanelWidth, setWidth: setRightPanelWidth } = usePersistedSidebarWidth({
        storageKey: rightPanelWidthLocalStorageKey,
        minWidth: MIN_RIGHT_PANEL_WIDTH,
        maxWidth: MAX_RIGHT_PANEL_WIDTH,
        defaultRatio: DEFAULT_RIGHT_PANEL_RATIO,
    });
    const [isDragging, setIsDragging] = useState(false);

    const rightPanel = useMemo(() => {
        if (!rightPanelContent) return null;
        if (!isRightPanelResizable) {
            return <SidePanel $closed={isRightPanelCollapsed}>{rightPanelContent}</SidePanel>;
        }
        return (
            <SidePanel $closed={isRightPanelCollapsed} $width={rightPanelWidth} $noTransition={isDragging} $isResizable>
                {!isRightPanelCollapsed && (
                    <SidebarResizer
                        edge="left"
                        minWidth={MIN_RIGHT_PANEL_WIDTH}
                        maxWidth={MAX_RIGHT_PANEL_WIDTH}
                        capToRow
                        onWidthChange={setRightPanelWidth}
                        onResizeStart={() => setIsDragging(true)}
                        onResizeEnd={() => setIsDragging(false)}
                    />
                )}
                {rightPanelContent}
            </SidePanel>
        );
    }, [
        rightPanelContent,
        isRightPanelResizable,
        isRightPanelCollapsed,
        setRightPanelWidth,
        rightPanelWidth,
        isDragging,
    ]);

    return (
        <VerticalContainer>
            <HorizontalContainer $isRightPanelCollapsed={isRightPanelCollapsed}>
                {leftPanelContent && <SidePanel>{leftPanelContent}</SidePanel>}

                <PageWrapper>
                    {(topBreadcrumb || topRightContent) && (
                        <TopContainer>
                            {topBreadcrumb && <>{topBreadcrumb}</>}
                            {topRightContent && <TopRightContentContainer>{topRightContent}</TopRightContentContainer>}
                        </TopContainer>
                    )}
                    {title && (
                        <PageTitleWrapper>
                            <PageTitle title={title} subTitle={subTitle} titlePill={titlePill} />
                        </PageTitleWrapper>
                    )}
                    <ContentWrapper>{children}</ContentWrapper>
                </PageWrapper>

                {rightPanel}
            </HorizontalContainer>
            {bottomPanelContent && <BottomPanel>{bottomPanelContent}</BottomPanel>}
        </VerticalContainer>
    );
}
