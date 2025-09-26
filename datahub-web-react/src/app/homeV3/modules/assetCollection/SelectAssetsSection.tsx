import React, { useCallback } from 'react';
import styled from 'styled-components';

import DynamicSelectAssetsTab from '@app/homeV3/modules/assetCollection/DynamicSelectAssetsTab';
import ManualSelectAssetsTab from '@app/homeV3/modules/assetCollection/ManualSelectAssetsTab';
import { SELECT_ASSET_TYPE_DYNAMIC, SELECT_ASSET_TYPE_MANUAL } from '@app/homeV3/modules/assetCollection/constants';
import ButtonTabs from '@app/homeV3/modules/shared/ButtonTabs/ButtonTabs';
import { Tab } from '@app/homeV3/modules/shared/ButtonTabs/types';
import { LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';

const AssetsSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

type Props = {
    selectAssetType: string;
    setSelectAssetType: (newSelectAssetType: string) => void;
    selectedAssetUrns: string[];
    setSelectedAssetUrns: React.Dispatch<React.SetStateAction<string[]>>;
    dynamicFilter: LogicalPredicate | null | undefined;
    setDynamicFilter: (newDynamicFilter: LogicalPredicate | null | undefined) => void;
};

const SelectAssetsSection = ({
    selectAssetType,
    setSelectAssetType,
    selectedAssetUrns,
    setSelectedAssetUrns,
    dynamicFilter,
    setDynamicFilter,
}: Props) => {
    const tabs: Tab[] = [
        {
            key: SELECT_ASSET_TYPE_MANUAL,
            label: 'Select Assets',
            content: (
                <ManualSelectAssetsTab
                    selectedAssetUrns={selectedAssetUrns}
                    setSelectedAssetUrns={setSelectedAssetUrns}
                />
            ),
        },
        {
            key: SELECT_ASSET_TYPE_DYNAMIC,
            label: 'Dynamic Filter',
            content: <DynamicSelectAssetsTab dynamicFilter={dynamicFilter} setDynamicFilter={setDynamicFilter} />,
        },
    ];

    const onTabChanged = useCallback(
        (newActiveTabKey: string) => {
            if (newActiveTabKey === SELECT_ASSET_TYPE_MANUAL || newActiveTabKey === SELECT_ASSET_TYPE_DYNAMIC) {
                setSelectAssetType?.(newActiveTabKey);
            }
        },
        [setSelectAssetType],
    );

    return (
        <AssetsSection>
            <ButtonTabs tabs={tabs} onTabClick={onTabChanged} defaultKey={selectAssetType} />
        </AssetsSection>
    );
};

export default SelectAssetsSection;
