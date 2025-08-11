import { Checkbox, Loader, SearchBar, Text } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import EntityItem from '@app/homeV3/module/components/EntityItem';
import AssetFilters from '@app/homeV3/modules/assetCollection/AssetFilters';
import EmptySection from '@app/homeV3/modules/assetCollection/EmptySection';
import useGetAssetResults from '@app/homeV3/modules/assetCollection/useGetAssetResults';
import { LoaderContainer } from '@app/homeV3/styledComponents';
import { getEntityDisplayType } from '@app/searchV2/autoCompleteV2/utils';
import useAppliedFilters from '@app/searchV2/filtersV2/context/useAppliedFilters';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { DataHubPageModuleType, Entity } from '@types';

const AssetsSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const ItemDetailsContainer = styled.div`
    display: flex;
    align-items: center;
`;

const ResultsContainer = styled.div`
    margin: 0 -16px 0 -8px;
`;

type Props = {
    selectedAssetUrns: string[];
    setSelectedAssetUrns: React.Dispatch<React.SetStateAction<string[]>>;
};

const SelectAssetsSection = ({ selectedAssetUrns, setSelectedAssetUrns }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const [searchQuery, setSearchQuery] = useState<string | undefined>();
    const { appliedFilters, updateFieldFilters } = useAppliedFilters();
    const { entities, loading } = useGetAssetResults({ searchQuery, appliedFilters });

    const handleSearchChange = (value: string) => {
        setSearchQuery(value);
    };

    const handleCheckboxChange = (urn: string) => {
        setSelectedAssetUrns((prev) => (prev.includes(urn) ? prev.filter((u) => u !== urn) : [...prev, urn]));
    };

    const customDetailsRenderer = (entity: Entity) => {
        const displayType = getEntityDisplayType(entity, entityRegistry);

        return (
            <ItemDetailsContainer>
                <Text color="gray" size="sm">
                    {displayType}
                </Text>
                <Checkbox
                    size="xs"
                    isChecked={selectedAssetUrns?.includes(entity.urn)}
                    onCheckboxChange={() => handleCheckboxChange(entity.urn)}
                />
            </ItemDetailsContainer>
        );
    };

    let content;
    if (loading) {
        content = (
            <LoaderContainer>
                <Loader />
            </LoaderContainer>
        );
    } else if (entities && entities.length > 0) {
        content = entities?.map((entity) => (
            <EntityItem
                entity={entity}
                key={entity.urn}
                customDetailsRenderer={customDetailsRenderer}
                moduleType={DataHubPageModuleType.AssetCollection}
                navigateOnlyOnNameClick
            />
        ));
    } else {
        content = <EmptySection />;
    }

    return (
        <AssetsSection>
            <Text color="gray" weight="bold">
                Search and Select Assets
            </Text>
            <SearchBar value={searchQuery} onChange={handleSearchChange} />
            <AssetFilters
                searchQuery={searchQuery}
                appliedFilters={appliedFilters}
                updateFieldFilters={updateFieldFilters}
            />
            <ResultsContainer>{content}</ResultsContainer>
        </AssetsSection>
    );
};

export default SelectAssetsSection;
