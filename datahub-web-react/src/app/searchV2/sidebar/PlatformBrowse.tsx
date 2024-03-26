import React, { useEffect } from 'react';
import styled from 'styled-components';
import { Divider, Empty } from 'antd';
import { BrowseProvider } from './BrowseContext';
import SidebarLoadingError from './SidebarLoadingError';
import useSidebarPlatforms from './useSidebarPlatforms';
import PlatformNode from './PlatformNode';

const BrowsePlatformIcons = styled.div`
    display: flex;
    flex-direction: column;
`;

const DividerStyle = styled(Divider)`
    margin: unset;
`;

type Props = {
    visible: boolean;
    collapsed?: boolean;
    expand: () => void;
    closeSidebar: () => void;
};

const PlatformBrowse = ({ visible, collapsed = false, expand, closeSidebar }: Props) => {
    const { error, platformAggregations, retry } = useSidebarPlatforms({
        skip: !visible,
    });
    const isEmpty =
        (platformAggregations === null || (platformAggregations && !platformAggregations.length)) && !collapsed;

    useEffect(() => {
        if (platformAggregations === null || platformAggregations?.length === 0) {
            closeSidebar();
        }
    }, [platformAggregations, closeSidebar]);

    return (
        <>
            {isEmpty && <Empty description="No matching platforms found" image={Empty.PRESENTED_IMAGE_SIMPLE} />}
            <BrowsePlatformIcons>
                {platformAggregations
                    ?.sort((a, b) => b.count - a.count)
                    ?.map((platformAggregation, i, lst) => (
                        <BrowseProvider key={platformAggregation.value} platformAggregation={platformAggregation}>
                            <PlatformNode
                                iconSize={24}
                                hasOnlyOnePlatform={lst.length === 1}
                                toggleCollapse={expand}
                                collapsed={collapsed}
                            />
                            {platformAggregation && i < platformAggregations.length - 1 && <DividerStyle />}
                        </BrowseProvider>
                    ))}
            </BrowsePlatformIcons>
            {error && <SidebarLoadingError onClickRetry={retry} />}
        </>
    );
};

export default PlatformBrowse;
