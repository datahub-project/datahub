import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { SelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetGlossaryNodeLazyQuery } from '@src/graphql/glossaryNode.generated';
import {
    useGetAutoCompleteMultipleResultsLazyQuery,
    useGetSearchResultsForMultipleQuery,
} from '@src/graphql/search.generated';
import { EntityType, GlossaryTerm } from '@src/types.generated';
import { Form } from 'antd';
import React, { useState } from 'react';
import { SelectorWrapper } from '../styledComponents';

const GlossaryTermsSelector = () => {
    const entityRegistry = useEntityRegistryV2();
    const [useSearch, setUseSearch] = useState(false);
    const form = Form.useFormInstance();
    const initialAllowedTerms =
        form.getFieldValue(['glossaryTermsParams', 'allowedTerms']) ||
        form.getFieldValue(['glossaryTermsParams', 'resolvedAllowedTerms']) ||
        [];
    const initialOptions = initialAllowedTerms.map((term: GlossaryTerm) => ({
        value: term.urn,
        label: entityRegistry.getDisplayName(term.type, term),
        id: term.urn,
        isParent: term.type === EntityType.GlossaryNode,
        parentId: term.parentNodes?.[0]?.urn,
        entity: term,
    }));

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
                        entity,
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

    function handleUpdate(values: SelectOption[]) {
        if (values.length) {
            const allowedTerms = values.map((v) => v.entity).filter((r) => !!r);
            form.setFieldValue(['glossaryTermsParams', 'allowedTerms'], allowedTerms);
        } else {
            form.setFieldValue(['glossaryTermsParams', 'allowedTerms'], undefined);
        }
    }

    return (
        <SelectorWrapper>
            <NestedSelect
                label="Allowed Glossary Terms:"
                placeholder="Select allowed glossary terms"
                searchPlaceholder="Search all glossary terms..."
                areParentsSelectable={false}
                options={useSearch ? autoCompleteOptions : [...options, ...childOptions]}
                initialValues={initialOptions}
                loadData={handleLoad}
                onSearch={handleSearch}
                onUpdate={handleUpdate}
                width="full"
                isMultiSelect
                showSearch
            />
        </SelectorWrapper>
    );
};

export default GlossaryTermsSelector;
