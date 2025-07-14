import { Checkbox, SearchBar, Text } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import EntityItem from '@app/homeV3/module/components/EntityItem';
import AssetFilters from '@app/homeV3/modules/assetCollection/AssetFilters';
import useGetAssetResults from '@app/homeV3/modules/assetCollection/useGetAssetResults';
import { getEntityDisplayType } from '@app/searchV2/autoCompleteV2/utils';
import { FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Entity } from '@types';

const AssetSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

type Props = {
    selectedAssetUrns: string[];
    setSelectedAssetUrns: React.Dispatch<React.SetStateAction<string[]>>;
};

const SelectAssetsSection = ({ selectedAssetUrns, setSelectedAssetUrns }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const [searchQuery, setSearchQuery] = useState<string | undefined>();
    const [appliedFilters, setAppliedFilters] = useState<FieldToAppliedFieldFiltersMap>(new Map());

    const { entities } = useGetAssetResults({ searchQuery, appliedFilters });

    const handleSearchChange = (value: string) => {
        setSearchQuery(value);
    };

    const handleCheckboxChange = (urn: string) => {
        setSelectedAssetUrns((prev) => (prev.includes(urn) ? prev.filter((u) => u !== urn) : [...prev, urn]));
    };

    const customDetailsRenderer = (entity: Entity) => {
        const displayType = getEntityDisplayType(entity, entityRegistry);

        return (
            <>
                <Text color="gray" size="sm">
                    {displayType}
                </Text>
                <Checkbox
                    isChecked={selectedAssetUrns?.includes(entity.urn)}
                    onCheckboxChange={() => handleCheckboxChange(entity.urn)}
                />
            </>
        );
    };

    return (
        <AssetSection>
            <Text color="gray" weight="bold">
                Search and Select Assets
            </Text>
            <SearchBar value={searchQuery} onChange={handleSearchChange} />
            <AssetFilters
                searchQuery={searchQuery}
                appliedFilters={appliedFilters}
                setAppliedFilters={setAppliedFilters}
            />
            {entities?.map((entity) => (
                <EntityItem entity={entity} key={entity.urn} customDetailsRenderer={customDetailsRenderer} />
            ))}
        </AssetSection>
    );
};

export default SelectAssetsSection;
