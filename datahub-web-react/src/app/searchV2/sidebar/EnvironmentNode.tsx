import { Globe } from '@phosphor-icons/react/dist/csr/Globe';
import React from 'react';
import { useTheme } from 'styled-components';

import {
    BrowseProvider,
    useEntityAggregation,
    useEnvironmentAggregation,
    useIsEnvironmentSelected,
} from '@app/searchV2/sidebar/BrowseContext';
import PlatformNode from '@app/searchV2/sidebar/PlatformNode';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useAggregationsQuery from '@app/searchV2/sidebar/useAggregationsQuery';
import useSidebarAnalytics from '@app/searchV2/sidebar/useSidebarAnalytics';
import { PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import useToggle from '@app/shared/useToggle';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';

const EnvironmentNode = () => {
    const theme = useTheme();
    const isSelected = useIsEnvironmentSelected();
    const entityAggregation = useEntityAggregation();
    const environmentAggregation = useEnvironmentAggregation();
    const { count } = environmentAggregation;
    const label = environmentAggregation?.value;
    const { trackToggleNodeEvent } = useSidebarAnalytics();
    const { isOpen, isClosing, toggle } = useToggle({
        initialValue: isSelected,
        closeDelay: 250,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'environment'),
    });

    const onToggle = () => {
        if (count) toggle();
    };

    const { loaded, error, platformAggregations, retry } = useAggregationsQuery({
        skip: !isOpen,
        facets: [PLATFORM_FILTER_NAME],
    });

    const showChildren = isOpen && !isClosing && loaded;

    return (
        <>
            <HierarchicalBrowseTreeRow
                level={1}
                isSelected={isSelected}
                hasChildren={!!count}
                isExpanded={isOpen && !isClosing}
                isLoadingChildren={isOpen && !loaded}
                count={count}
                icon={<Globe size={TREE_ROW_ENTITY_ICON_SIZE} color={theme.colors.icon} />}
                label={label}
                labelTitle={label}
                onSelect={onToggle}
                onToggleExpand={onToggle}
            />
            {showChildren && (
                <>
                    {platformAggregations?.map((platformAggregation) => (
                        <BrowseProvider
                            key={platformAggregation.value}
                            entityAggregation={entityAggregation}
                            environmentAggregation={environmentAggregation}
                            platformAggregation={platformAggregation}
                        >
                            <PlatformNode level={2} />
                        </BrowseProvider>
                    ))}
                    {error && <SidebarLoadingError onClickRetry={retry} />}
                </>
            )}
        </>
    );
};

export default EnvironmentNode;
