import { Checkbox, Loader, SearchBar, Text } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import EntityItem from '@app/homeV3/module/components/EntityItem';
import AssetFilters from '@app/homeV3/modules/assetCollection/AssetFilters';
import EmptySection from '@app/homeV3/modules/assetCollection/EmptySection';
import useGetAssetResults from '@app/homeV3/modules/assetCollection/useGetAssetResults';
import { LoaderContainer } from '@app/homeV3/styledComponents';
import { getEntityDisplayType } from '@app/searchV2/autoCompleteV2/utils';
import { FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Entity } from '@types';

const AssetSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    height: 100%;
`;

type Props = {
    selectedAssetUrns: string[];
    setSelectedAssetUrns: React.Dispatch<React.SetStateAction<string[]>>;
};

const SelectAssetsSection = ({ selectedAssetUrns, setSelectedAssetUrns }: Props) => {
    const entityRegistry = useEntityRegistryV2();

    const [searchQuery, setSearchQuery] = useState<string | undefined>();
    const [appliedFilters, setAppliedFilters] = useState<FieldToAppliedFieldFiltersMap>(new Map());

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
            <>
                <Text color="gray" size="sm">
                    {displayType}
                </Text>
                <Checkbox
                    size="sm"
                    isChecked={selectedAssetUrns?.includes(entity.urn)}
                    onCheckboxChange={() => handleCheckboxChange(entity.urn)}
                />
            </>
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
            <EntityItem entity={entity} key={entity.urn} customDetailsRenderer={customDetailsRenderer} />
        ));
    } else {
        content = <EmptySection />;
    }

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
            {content}
        </AssetSection>
    );
};

export default SelectAssetsSection;
