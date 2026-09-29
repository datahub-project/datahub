import { PageTitle } from '@components';
import React, { useCallback, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import { PanelResizeHandle } from '@app/sharedV2/layouts/PanelResizeHandle';

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

const SidePanel = styled(Panel)<{ $closed?: boolean; $width?: number; $noTransition?: boolean }>`
    width: ${({ $closed, $width }) => {
        if ($closed) return '0px';
        if ($width) return `${$width}px`;
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
        if ($width) return `${$width}px`;
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

const MIN_RIGHT_PANEL_WIDTH = 320;
const MAX_RIGHT_PANEL_WIDTH_RATIO = 0.6;

function clampRightPanelWidth(width: number): number {
    const max = window.innerWidth * MAX_RIGHT_PANEL_WIDTH_RATIO;
    return Math.min(Math.max(width, MIN_RIGHT_PANEL_WIDTH), max);
}

const BottomPanel = styled(Panel)`
    height: 62px;
`;

const VerticalContainer = styled.div`
    display: flex;
    flex: 1;
    flex-direction: column;
    gap: 16px;
    /* Without this, one wide unbreakable descendant (a code block in the chat
       rail, a long link) inflates this column's min-content past the app row
       and the whole layout lays out wider than the viewport. */
    min-width: 0;
    min-height: 0;
    overflow: hidden;
`;

const HorizontalContainer = styled.div<{ $hasBottomPanel?: boolean; $isRightPanelCollapsed?: boolean }>`
    flex: 1;
    display: flex;
    flex-direction: row;
    gap: 8px;
    min-width: 0;
    min-height: 0;
    overflow: hidden;
    max-height: calc(100vh - ${(props) => (props.$hasBottomPanel ? '156px' : '78px')});
    ${(props) =>
        props.$isRightPanelCollapsed &&
        `
            margin-right: -8px;
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
    /** When set, the resized right panel width is persisted to localStorage under this key. */
    rightPanelWidthLocalStorageKey?: string;
}

function readStoredWidth(key: string | undefined): number | undefined {
    if (!key) return undefined;
    const stored = window.localStorage.getItem(key);
    const parsed = stored ? Number(stored) : NaN;
    return Number.isFinite(parsed) ? parsed : undefined;
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
    const sidePanelRef = useRef<HTMLDivElement>(null);
    const [rightPanelWidth, setRightPanelWidth] = useState<number | undefined>(() =>
        readStoredWidth(rightPanelWidthLocalStorageKey),
    );
    const [isDragging, setIsDragging] = useState(false);

    const getInitialWidth = useCallback(
        () => rightPanelWidth ?? sidePanelRef.current?.offsetWidth ?? MIN_RIGHT_PANEL_WIDTH,
        [rightPanelWidth],
    );

    const handleResize = useCallback((width: number) => {
        setIsDragging(true);
        setRightPanelWidth(clampRightPanelWidth(width));
    }, []);

    const handleResizeEnd = useCallback(
        (width: number) => {
            setIsDragging(false);
            const clamped = clampRightPanelWidth(width);
            setRightPanelWidth(clamped);
            if (rightPanelWidthLocalStorageKey) {
                window.localStorage.setItem(rightPanelWidthLocalStorageKey, String(clamped));
            }
        },
        [rightPanelWidthLocalStorageKey],
    );

    const rightPanel = useMemo(() => {
        if (!rightPanelContent) return null;
        if (!isRightPanelResizable) {
            return <SidePanel $closed={isRightPanelCollapsed}>{rightPanelContent}</SidePanel>;
        }
        // Rendered as siblings (not wrapped in a div) so SidePanel remains a direct flex child of
        // HorizontalContainer — its percentage/pixel width is computed against that container, not
        // against an intermediate wrapper with no defined width of its own.
        return (
            <>
                {!isRightPanelCollapsed && (
                    <PanelResizeHandle
                        getInitialWidth={getInitialWidth}
                        onResize={handleResize}
                        onResizeEnd={handleResizeEnd}
                    />
                )}
                <SidePanel
                    ref={sidePanelRef}
                    $closed={isRightPanelCollapsed}
                    $width={rightPanelWidth}
                    $noTransition={isDragging}
                >
                    {rightPanelContent}
                </SidePanel>
            </>
        );
    }, [
        rightPanelContent,
        isRightPanelResizable,
        isRightPanelCollapsed,
        getInitialWidth,
        handleResize,
        handleResizeEnd,
        rightPanelWidth,
        isDragging,
    ]);

    return (
        <VerticalContainer>
            <HorizontalContainer $hasBottomPanel={!!bottomPanelContent} $isRightPanelCollapsed={isRightPanelCollapsed}>
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
