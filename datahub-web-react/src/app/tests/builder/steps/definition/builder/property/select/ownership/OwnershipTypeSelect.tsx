import { Select } from 'antd';
import React, { useEffect } from 'react';

import { OwnershipTypeSelectResult } from '@app/tests/builder/steps/definition/builder/property/select/ownership/OwnershipTypeSelectResult';
import { createOwnershipTypeUrnMap } from '@app/tests/builder/steps/definition/builder/property/select/ownership/utils';

import { useSearchOwnershipTypesLazyQuery } from '@graphql/ownershipTypes.generated';
import { OwnershipTypeEntity } from '@types';

type Types = {
    selectedOwnershipType?: OwnershipTypeEntity; // The selected ownership type
    onSelect: (ownershipType: OwnershipTypeEntity) => void;
    onClear: () => void;
};

/**
 * Select a ownership type from a dropdown. Allow search values.
 */
export const OwnershipTypeSelect = ({ selectedOwnershipType, onSelect, onClear }: Types) => {
    const [searchOwnershipTypes, { data: ownershipTypesSearchData }] = useSearchOwnershipTypesLazyQuery();
    const searchResults = ownershipTypesSearchData?.searchAcrossEntities?.searchResults || [];
    const ownershipTypes = searchResults.map((result) => result.entity) as OwnershipTypeEntity[];

    // Local of ownership type urn to ownership type object.
    const urnToOwnershipType = createOwnershipTypeUrnMap(ownershipTypes);

    const onSearch = (text: string) => {
        searchOwnershipTypes({
            variables: {
                query: text,
                start: 0,
                count: 10,
            },
        });
    };

    useEffect(() => {
        searchOwnershipTypes({
            variables: {
                query: '*',
                start: 0,
                count: 20,
            },
        });
    }, [searchOwnershipTypes]);

    return (
        <Select
            autoFocus
            allowClear
            autoClearSearchValue={false}
            showSearch={!selectedOwnershipType?.urn}
            maxTagCount={1}
            style={{ width: 240, marginRight: 8 }}
            dropdownMatchSelectWidth={400}
            value={(selectedOwnershipType?.urn && [selectedOwnershipType?.urn]) || []}
            filterOption={false}
            placeholder="Search for ownership types..."
            onSelect={(ownershipTypeUrn) => {
                onSelect(urnToOwnershipType.get(ownershipTypeUrn));
            }}
            onSearch={onSearch}
            onClear={onClear}
            showArrow={false}
        >
            {ownershipTypes?.map((ownershipType) => (
                <Select.Option value={ownershipType.urn}>
                    <OwnershipTypeSelectResult ownershipType={ownershipType} />
                </Select.Option>
            ))}
        </Select>
    );
};
