import { EmptyState } from '@components';
import { HardDrives } from '@phosphor-icons/react/dist/csr/HardDrives';
import React, { useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { BrowseProvider } from '@app/searchV2/sidebar/BrowseContext';
import PlatformNode from '@app/searchV2/sidebar/PlatformNode';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useSidebarPlatforms from '@app/searchV2/sidebar/useSidebarPlatforms';

const BrowsePlatformIcons = styled.div`
    display: flex;
    flex-direction: column;
`;

const EmptyStateWrapper = styled.div`
    padding: 24px 12px;
`;

type Props = {
    visible: boolean;
    collapsed?: boolean;
    expand: () => void;
    hideSidebar: () => void;
    unhideSidebar: () => void;
};

const PlatformBrowse = ({ visible, collapsed = false, expand, hideSidebar, unhideSidebar }: Props) => {
    const { t } = useTranslation('search');
    const { error, platformAggregations, retry } = useSidebarPlatforms({
        skip: !visible,
    });
    const isEmpty =
        (platformAggregations === null || (platformAggregations && !platformAggregations.length)) && !collapsed;
    const sortedPlatforms = [...(platformAggregations ?? [])].sort((a, b) => b.count - a.count);

    useEffect(() => {
        if (platformAggregations === null || platformAggregations?.length === 0) {
            hideSidebar();
        } else {
            unhideSidebar();
        }
    }, [platformAggregations, hideSidebar, unhideSidebar]);

    return (
        <>
            {isEmpty && (
                <EmptyStateWrapper>
                    <EmptyState icon={HardDrives} title={t('sidebar.noMatchingPlatforms')} size="sm" />
                </EmptyStateWrapper>
            )}
            <BrowsePlatformIcons>
                {sortedPlatforms.map((platformAggregation) => (
                    <BrowseProvider key={platformAggregation.value} platformAggregation={platformAggregation}>
                        <PlatformNode
                            hasOnlyOnePlatform={sortedPlatforms.length === 1}
                            toggleCollapse={expand}
                            collapsed={collapsed}
                        />
                    </BrowseProvider>
                ))}
            </BrowsePlatformIcons>
            {error && <SidebarLoadingError onClickRetry={retry} />}
        </>
    );
};

export default PlatformBrowse;
