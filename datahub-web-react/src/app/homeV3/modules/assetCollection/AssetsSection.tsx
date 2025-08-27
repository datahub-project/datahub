import { colors } from '@components';
import { Divider } from 'antd';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import SelectAssetsSection from '@app/homeV3/modules/assetCollection/SelectAssetsSection';
import SelectedAssetsSection from '@app/homeV3/modules/assetCollection/SelectedAssetsSection';
import { SELECT_ASSET_TYPE_MANUAL } from '@app/homeV3/modules/assetCollection/constants';
import { LogicalPredicate } from '@app/sharedV2/queryBuilder/builder/types';

const Container = styled.div`
    display: flex;
    width: 100%;
    gap: 8px;
`;

const LeftSection = styled.div`
    flex: 6;
`;

const RightSection = styled.div`
    flex: 4;
    width: calc(40% - 20px);
`;

export const VerticalDivider = styled(Divider)`
    color: ${colors.gray[100]};
    height: auto;
`;

type Props = {
    selectAssetType: string;
    setSelectAssetType: (newSelectAssetType: string) => void;
    selectedAssetUrns: string[];
    setSelectedAssetUrns: React.Dispatch<React.SetStateAction<string[]>>;
    dynamicFilter: LogicalPredicate | null | undefined;
    setDynamicFilter: (newDynamicFilter: LogicalPredicate | null | undefined) => void;
};

const AssetsSection = ({
    selectAssetType,
    setSelectAssetType,
    selectedAssetUrns,
    setSelectedAssetUrns,
    dynamicFilter,
    setDynamicFilter,
}: Props) => {
    const shouldShowSelectedAssetsSection = useMemo(
        () => selectAssetType === SELECT_ASSET_TYPE_MANUAL,
        [selectAssetType],
    );

    return (
        <Container>
            <LeftSection>
                <SelectAssetsSection
                    selectAssetType={selectAssetType}
                    setSelectAssetType={setSelectAssetType}
                    selectedAssetUrns={selectedAssetUrns}
                    setSelectedAssetUrns={setSelectedAssetUrns}
                    dynamicFilter={dynamicFilter}
                    setDynamicFilter={setDynamicFilter}
                />
            </LeftSection>
            <VerticalDivider type="vertical" />
            {shouldShowSelectedAssetsSection && (
                <RightSection>
                    <SelectedAssetsSection
                        selectedAssetUrns={selectedAssetUrns}
                        setSelectedAssetUrns={setSelectedAssetUrns}
                    />
                </RightSection>
            )}
        </Container>
    );
};

export default AssetsSection;
