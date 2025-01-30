export type GraphCardProps = {
    title: string;
    subTitle?: string | React.ReactNode;
    loading?: boolean;
    graphHeight?: string;
    width?: string;
    renderGraph: () => React.ReactNode;
    renderControls?: () => React.ReactNode;
    isEmpty?: boolean;
    emptyContent?: React.ReactNode;
    moreInfoModalContent?: React.ReactNode;
};
