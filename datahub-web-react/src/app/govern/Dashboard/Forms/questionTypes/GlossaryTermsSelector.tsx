import { EntityType } from '@src/types.generated';
import React, { useState } from 'react';
import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import {
    useGetSearchResultsForMultipleQuery,
    useGetAutoCompleteMultipleResultsLazyQuery,
} from '@src/graphql/search.generated';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetGlossaryNodeLazyQuery } from '@src/graphql/glossaryNode.generated';
import styled from 'styled-components';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';

const Wrapper = styled.div`
    margin-bottom: 24px;

    label {
        font-size: 14px !important;
    }
`;

const GlossaryTermsSelector = () => {
    const entityRegistry = useEntityRegistryV2();
    const [useSearch, setUseSearch] = useState(false);

    const [autoComplete, { data: autoCompleteData }] = useGetAutoCompleteMultipleResultsLazyQuery();
    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                query: '*',
                types: [EntityType.GlossaryNode, EntityType.GlossaryTerm],
                orFilters: [{ and: [{ field: 'hasParentNode', value: 'false' }] }],
            },
        },
    });

    const [seenUrns] = useState<Set<string>>(new Set());
    const [childOptions, setChildOptions] = useState<SelectOption[]>([]);

    const [getNode] = useGetGlossaryNodeLazyQuery({
        onCompleted: (nodeData) => {
            const childOptionsToAdd: SelectOption[] = [];
            nodeData.glossaryNode?.children?.relationships.forEach((relationship) => {
                if (relationship.entity && !seenUrns.has(relationship.entity.urn)) {
                    const { entity } = relationship;
                    const { urn, type } = entity;
                    seenUrns.add(urn);
                    childOptionsToAdd.push({
                        value: urn,
                        label: entityRegistry.getDisplayName(type, entity),
                        isParent: type === EntityType.GlossaryNode,
                        parentValue: nodeData.glossaryNode?.urn,
                    });
                }
            });
            setChildOptions((existingOptions) => [...existingOptions, ...childOptionsToAdd]);
        },
    });

    const options =
        data?.searchAcrossEntities?.searchResults.map((result) => ({
            value: result.entity.urn,
            label: entityRegistry.getDisplayName(result.entity.type, result.entity),
            isParent: result.entity.type === EntityType.GlossaryNode,
        })) || [];
    const autoCompleteOptions =
        autoCompleteData?.autoCompleteForMultiple?.suggestions
            .find((s) => s.type === EntityType.GlossaryTerm)
            ?.entities.map((term) => ({
                value: term.urn,
                label: entityRegistry.getDisplayName(term.type, term),
                isParent: term.type === EntityType.GlossaryNode,
            })) || [];

    function handleLoad(option: SelectOption) {
        getNode({ variables: { urn: option.value } });
    }

    function handleSearch(query: string) {
        if (query) {
            autoComplete({ variables: { input: { query, types: [EntityType.GlossaryTerm] } } });
            setUseSearch(true);
        } else {
            setUseSearch(false);
        }
    }

    return (
        <Wrapper>
            <NestedSelect
                label="Glossary Terms"
                placeholder="Select allowed glossary terms"
                areParentsSelectable={false}
                options={useSearch ? autoCompleteOptions : [...options, ...childOptions]}
                loadData={handleLoad}
                onSearch={handleSearch}
                width="full"
                isMultiSelect
                showSearch
            />
        </Wrapper>
    );
};

export default GlossaryTermsSelector;
