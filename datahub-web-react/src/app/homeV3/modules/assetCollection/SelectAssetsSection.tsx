import { SearchBar, Text } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import EntityItem from '@app/homeV3/module/components/EntityItem';
import AssetFilters from '@app/homeV3/modules/assetCollection/AssetFilters';
import useGetAssetResults from '@app/homeV3/modules/assetCollection/useGetAssetResults';
import { FieldToAppliedFieldFiltersMap } from '@app/searchV2/filtersV2/types';

const AssetSection = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const SelectAssetsSection = () => {
    const [searchQuery, setSearchQuery] = useState<string | undefined>();
    const [appliedFilters, setAppliedFilters] = useState<FieldToAppliedFieldFiltersMap>(new Map());

    const { entities } = useGetAssetResults({ searchQuery, appliedFilters });

    const handleSearchChange = (value: string) => {
        setSearchQuery(value);
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
            {entities?.map((entity) => <EntityItem entity={entity} key={entity.urn} />)}
        </AssetSection>
    );
};

export default SelectAssetsSection;
