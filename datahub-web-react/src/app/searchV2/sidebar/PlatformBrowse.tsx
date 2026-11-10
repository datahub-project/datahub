import { EmptyState } from '@components';
import { Tray } from '@phosphor-icons/react/dist/csr/Tray';
import React, { useEffect } from 'react';
import { useTranslation } from 'react-i18next';

import { BrowseProvider } from '@app/searchV2/sidebar/BrowseContext';
import PlatformNode from '@app/searchV2/sidebar/PlatformNode';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useSidebarPlatforms from '@app/searchV2/sidebar/useSidebarPlatforms';

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

    useEffect(() => {
        if (platformAggregations === null || platformAggregations?.length === 0) {
            hideSidebar();
        } else {
            unhideSidebar();
        }
    }, [platformAggregations, hideSidebar, unhideSidebar]);

    return (
        <>
            {isEmpty && <EmptyState size="sm" icon={Tray} title={t('sidebar.noMatchingPlatforms')} />}
            {platformAggregations
                ?.sort((a, b) => b.count - a.count)
                ?.map((platformAggregation, _i, lst) => (
                    <BrowseProvider key={platformAggregation.value} platformAggregation={platformAggregation}>
                        <PlatformNode
                            hasOnlyOnePlatform={lst.length === 1}
                            toggleCollapse={expand}
                            collapsed={collapsed}
                        />
                    </BrowseProvider>
                ))}
            {error && <SidebarLoadingError onClickRetry={retry} />}
        </>
    );
};

export default PlatformBrowse;
