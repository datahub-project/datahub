import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import {
    useGetAutoCompleteMultipleResultsLazyQuery,
    useGetSearchResultsForMultipleQuery,
} from '@src/graphql/search.generated';
import { Entity, EntityType } from '@src/types.generated';
import { Form } from 'antd';
import React, { useState } from 'react';
import { SelectorWrapper } from '../styledComponents';

const OwnershipSelector = () => {
    const entityRegistry = useEntityRegistryV2();
    const [useSearch, setUseSearch] = useState(false);
    const form = Form.useFormInstance();
    const initialAllowedOwners = form.getFieldValue(['ownershipParams', 'allowedOwners']) || [];
    const initialOptions = initialAllowedOwners.map((owner: Entity) => ({
        value: owner.urn,
        label: entityRegistry.getDisplayName(owner.type, owner),
        id: owner.urn,
        entity: owner,
    }));

    const [autoComplete, { data: autoCompleteData }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                types: [EntityType.CorpUser, EntityType.CorpGroup],
                count: 5,
            },
        },
    });

    const options =
        data?.searchAcrossEntities?.searchResults.map((result) => ({
            value: result.entity.urn,
            label: entityRegistry.getDisplayName(result.entity.type, result.entity),
            id: result.entity.urn,
            entity: result.entity,
        })) || [];
    const autoCompleteOptions =
        autoCompleteData?.autoCompleteForMultiple?.suggestions.flatMap((s) =>
            s.entities.map((entity) => ({
                value: entity.urn,
                label: entityRegistry.getDisplayName(entity.type, entity),
                id: entity.urn,
                entity,
            })),
        ) || [];

    function handleSearch(query: string) {
        if (query) {
            autoComplete({ variables: { input: { query, types: [EntityType.CorpUser, EntityType.CorpGroup] } } });
            setUseSearch(true);
        } else {
            setUseSearch(false);
        }
    }

    function handleUpdate(values: NestedSelectOption[]) {
        if (values.length) {
            const allowedOwners = values.map((v) => v.entity).filter((r) => !!r);
            form.setFieldValue(['ownershipParams', 'allowedOwners'], allowedOwners);
        } else {
            form.setFieldValue(['ownershipParams', 'allowedOwners'], undefined);
        }
    }

    return (
        <SelectorWrapper>
            <NestedSelect
                label="Allowed Owners:"
                placeholder="Select allowed owners"
                searchPlaceholder="Search all owners..."
                options={useSearch ? autoCompleteOptions : options}
                initialValues={initialOptions}
                onSearch={handleSearch}
                onUpdate={handleUpdate}
                width="full"
                isMultiSelect
                showSearch
            />
        </SelectorWrapper>
    );
};

export default OwnershipSelector;
