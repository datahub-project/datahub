export type GraphCardProps = {
    title: string;
    /** Optional element rendered beside the title (e.g. a badge or info tooltip icon). */
    titlePill?: React.ReactNode;
    subTitle?: string | React.ReactNode;
    loading?: boolean;
    graphHeight?: string;
    width?: string;
    /** Gap between the card header and graph body. Forwarded to CardContainer. */
    gap?: string;
    renderGraph: () => React.ReactNode;
    renderControls?: () => React.ReactNode;
    isEmpty?: boolean;
    emptyContent?: React.ReactNode;
    moreInfoModalContent?: React.ReactNode;
    showHeader?: boolean;
    showEmptyMessageHeader?: boolean;
    emptyMessage?: string;
    dataTestId?: string;
};
