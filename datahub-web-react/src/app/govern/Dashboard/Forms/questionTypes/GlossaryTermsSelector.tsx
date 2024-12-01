import { EntityType } from '@src/types.generated';
import React, { useEffect, useState } from 'react';
import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetGlossaryNodeWithChildrenLazyQuery } from '@src/graphql/glossaryNode.generated';
import {
    useGetAutoCompleteMultipleResultsLazyQuery,
    useGetSearchResultsForMultipleQuery,
} from '@src/graphql/search.generated';
import styled from 'styled-components';
import { groupBy } from 'lodash';

const Wrapper = styled.div``;

type GlossaryTermSelectorProps = {
    initialOptions: any[];
    onUpdate: (values: SelectOption[]) => void;
    label?: string;
    placeholder?: string;
    areNodeSelectable?: boolean;
};

const GlossaryTermsSelector = ({
    initialOptions,
    onUpdate,
    label,
    placeholder,
    areNodeSelectable = false,
}: GlossaryTermSelectorProps) => {
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
    const [seenNodeUrns] = useState<Set<string>>(new Set());
    const [childOptions, setChildOptions] = useState<SelectOption[]>([]);

    const [getNode, { loading }] = useGetGlossaryNodeWithChildrenLazyQuery({
        onCompleted: (nodeData) => {
            const childOptionsToAdd: SelectOption[] = [];
            const alreadyPresentOptionUrns = childOptions.map((option) => option.value);
            const nodeUrn = nodeData.glossaryNode?.urn || '';
            seenNodeUrns.add(nodeUrn);
            nodeData.glossaryNode?.children?.relationships.forEach((relationship) => {
                if (relationship.entity && !seenUrns.has(relationship.entity.urn)) {
                    const { entity } = relationship;
                    const { urn, type } = entity;
                    seenUrns.add(urn);
                    if (!alreadyPresentOptionUrns.includes(urn)) {
                        childOptionsToAdd.push({
                            value: urn,
                            label: entityRegistry.getDisplayName(type, entity),
                            isParent: type === EntityType.GlossaryNode,
                            parentValue: nodeData.glossaryNode?.urn,
                            entity,
                        });
                    }
                }
            });
            setChildOptions((existingOptions) => [...existingOptions, ...childOptionsToAdd]);
        },
    });

    const assignInitialTermsToDropdown = () => {
        if (initialOptions.length) {
            const childOptionsToAdd: SelectOption[] = [];
            const nodeWiseTerms = groupBy(initialOptions, 'parentId');

            Object.keys(nodeWiseTerms).forEach((node) => {
                nodeWiseTerms[node].forEach((option) => {
                    if (!seenUrns.has(option.value)) {
                        seenUrns.add(option.value);
                        childOptionsToAdd.push({
                            ...option,
                            parentValue: node,
                        });
                    }
                });
            });
            setChildOptions((existingOptions) => [...existingOptions, ...childOptionsToAdd]);
        }
    };

    useEffect(() => {
        assignInitialTermsToDropdown();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [initialOptions]);

    const options =
        data?.searchAcrossEntities?.searchResults?.map((result) => ({
            value: result.entity.urn,
            label: result.entity.type ? entityRegistry.getDisplayName(result.entity.type, result.entity) : '',
            isParent: result.entity.type === EntityType.GlossaryNode,
            entity: result.entity,
        })) || [];
    const autoCompleteOptions =
        autoCompleteData?.autoCompleteForMultiple?.suggestions
            .find((s) => s.type === EntityType.GlossaryTerm)
            ?.entities.map((term) => ({
                value: term.urn,
                label: entityRegistry.getDisplayName(term.type, term),
                isParent: term.type === EntityType.GlossaryNode,
                entity: term,
            })) || [];

    function handleLoad(option: SelectOption) {
        if (!seenNodeUrns.has(option.value)) {
            getNode({ variables: { urn: option.value } });
        }
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
                label={label ?? ''}
                placeholder={placeholder || 'Select allowed glossary terms'}
                searchPlaceholder="Search all glossary terms..."
                areParentsSelectable={areNodeSelectable}
                options={useSearch ? autoCompleteOptions : [...options, ...childOptions]}
                initialValues={initialOptions}
                loadData={handleLoad}
                onSearch={handleSearch}
                onUpdate={onUpdate}
                width="full"
                isMultiSelect
                showSearch
                isLoadingParentChildList={loading}
            />
        </Wrapper>
    );
};

export default GlossaryTermsSelector;
