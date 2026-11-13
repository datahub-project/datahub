import { Stack } from '@phosphor-icons/react/dist/csr/Stack';
import React from 'react';

import {
    BrowseProvider,
    useEntityAggregation,
    useEnvironmentAggregation,
    useIsEnvironmentSelected,
} from '@app/searchV2/sidebar/BrowseContext';
import BrowseRowIcon from '@app/searchV2/sidebar/BrowseRowIcon';
import PlatformNode from '@app/searchV2/sidebar/PlatformNode';
import SidebarLoadingError from '@app/searchV2/sidebar/SidebarLoadingError';
import useAggregationsQuery from '@app/searchV2/sidebar/useAggregationsQuery';
import useSidebarAnalytics from '@app/searchV2/sidebar/useSidebarAnalytics';
import { PLATFORM_FILTER_NAME } from '@app/searchV2/utils/constants';
import HierarchicalBrowseTreeRow from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/HierarchicalBrowseTreeRow';
import { TREE_ROW_ENTITY_ICON_SIZE } from '@app/sharedV2/sidebar/HierarchicalBrowseSidebar/constants';
import useToggle from '@app/sharedV2/useToggle';

type Props = {
    level: number;
};

const EnvironmentNode = ({ level }: Props) => {
    const isSelected = useIsEnvironmentSelected();
    const entityAggregation = useEntityAggregation();
    const environmentAggregation = useEnvironmentAggregation();
    const { count } = environmentAggregation;
    const { trackToggleNodeEvent } = useSidebarAnalytics();
    const { isOpen, toggle } = useToggle({
        initialValue: isSelected,
        onToggle: (isNowOpen: boolean) => trackToggleNodeEvent(isNowOpen, 'environment'),
    });

    const onToggle = () => {
        if (count) toggle();
    };

    const { error, platformAggregations, retry } = useAggregationsQuery({
        skip: !isOpen,
        facets: [PLATFORM_FILTER_NAME],
    });

    return (
        <>
            <HierarchicalBrowseTreeRow
                level={level}
                isSelected={false}
                hasChildren={!!count}
                isExpanded={isOpen}
                count={count}
                icon={
                    <BrowseRowIcon>
                        <Stack size={TREE_ROW_ENTITY_ICON_SIZE} />
                    </BrowseRowIcon>
                }
                label={environmentAggregation?.value}
                labelTitle={environmentAggregation?.value}
                onSelect={onToggle}
                onToggleExpand={onToggle}
            />
            {isOpen && (
                <>
                    {platformAggregations?.map((platformAggregation) => (
                        <BrowseProvider
                            key={platformAggregation.value}
                            entityAggregation={entityAggregation}
                            environmentAggregation={environmentAggregation}
                            platformAggregation={platformAggregation}
                        >
                            <PlatformNode level={level + 1} />
                        </BrowseProvider>
                    ))}
                    {error && <SidebarLoadingError onClickRetry={retry} />}
                </>
            )}
        </>
    );
};

export default EnvironmentNode;
