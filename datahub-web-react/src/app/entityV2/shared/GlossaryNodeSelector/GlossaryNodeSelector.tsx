import React, { useEffect, useMemo, useState } from 'react';

import {
    buildEntityCache,
    entitiesToNestedSelectOptions,
    isEntityResolutionRequired,
    mergeSelectedNestedOptions,
} from '@app/entityV2/shared/utils/selectorUtils';
import { NestedSelect } from '@src/alchemy-components/components/Select/Nested/NestedSelect';
import { NestedSelectOption } from '@src/alchemy-components/components/Select/Nested/types';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { useGetEntitiesLazyQuery } from '@src/graphql/entity.generated';
import { useGetRootGlossaryNodesQuery } from '@src/graphql/glossary.generated';
import { useGetGlossaryNodeWithChildrenLazyQuery } from '@src/graphql/glossaryNode.generated';
import { useGetAutoCompleteMultipleResultsLazyQuery } from '@src/graphql/search.generated';
import { Entity, EntityType } from '@src/types.generated';

type GlossaryNodeSelectorProps = {
    selectedNodes: string[];
    onNodesChange: (nodeUrns: string[]) => void;
    placeholder?: string;
    label?: string;
    isMultiSelect?: boolean;
};

/**
 * Supports both single and multiple glossary node selection based on isMultiSelect prop
 */
const GlossaryNodeSelector: React.FC<GlossaryNodeSelectorProps> = ({
    selectedNodes,
    onNodesChange,
    placeholder = 'Select parent node',
    label = 'Parent',
    isMultiSelect = false,
}) => {
    const entityRegistry = useEntityRegistryV2();
    const [useSearch, setUseSearch] = useState(false);
    const [entityCache, setEntityCache] = useState<Map<string, Entity>>(new Map());

    // Entity hydration for selected nodes
    const [getEntities, { data: resolvedEntitiesData }] = useGetEntitiesLazyQuery();

    // Bootstrap by resolving all URNs that are not in the cache yet
    useEffect(() => {
        if (selectedNodes.length > 0 && isEntityResolutionRequired(selectedNodes, entityCache)) {
            getEntities({ variables: { urns: selectedNodes } });
        }
    }, [selectedNodes, entityCache, getEntities]);

    // Build cache from resolved entities
    useEffect(() => {
        if (resolvedEntitiesData && resolvedEntitiesData.entities?.length) {
            const entities: Entity[] = (resolvedEntitiesData?.entities as Entity[]) || [];
            setEntityCache(buildEntityCache(entities));
        }
    }, [resolvedEntitiesData]);

    const [autoComplete, { data: autoCompleteData }] = useGetAutoCompleteMultipleResultsLazyQuery();

    const { data } = useGetRootGlossaryNodesQuery({
        variables: {
            input: {
                start: 0,
                count: 1000,
            },
        },
    });

    // Convert selected node URNs to NestedSelectOption format using utility
    // Use useMemo to prevent unnecessary recalculations and ensure NestedSelect properly syncs
    const initialOptions = useMemo(() => {
        return entitiesToNestedSelectOptions(selectedNodes, entityCache, entityRegistry);
    }, [selectedNodes, entityCache, entityRegistry]);

    const [childOptions, setChildOptions] = useState<NestedSelectOption[]>([]);

    const [getNodeWithChildren] = useGetGlossaryNodeWithChildrenLazyQuery({
        onCompleted: (nodeData) => {
            const childOptionsToAdd: NestedSelectOption[] = [];
            const children = nodeData.glossaryNode?.children?.relationships || [];

            children.forEach((relationship) => {
                const node = relationship.entity;
                if (node && node.type === EntityType.GlossaryNode) {
                    const { urn, type } = node;
                    childOptionsToAdd.push({
                        value: urn,
                        label: entityRegistry.getDisplayName(type, node),
                        isParent: !!(node as any).childrenCount?.nodesCount,
                        parentValue: nodeData.glossaryNode?.urn,
                        entity: node,
                    });
                }
            });
            setChildOptions((existingOptions) => [...existingOptions, ...childOptionsToAdd]);
        },
    });

    const options =
        data?.getRootGlossaryNodes?.nodes?.map((node) => ({
            value: node.urn,
            label: entityRegistry.getDisplayName(node.type, node),
            id: node.urn,
            isParent: !!node.childrenCount?.nodesCount,
            entity: node,
        })) || [];

    const autoCompleteOptions =
        autoCompleteData?.autoCompleteForMultiple?.suggestions?.flatMap((s) =>
            s.entities
                .filter((e) => e.type === EntityType.GlossaryNode)
                .map((node) => ({
                    value: node.urn,
                    label: entityRegistry.getDisplayName(node.type, node),
                    id: node.urn,
                    entity: node,
                })),
        ) || [];

    function handleLoad(option: NestedSelectOption) {
        getNodeWithChildren({ variables: { urn: option.value } });
    }

    function handleSearch(query: string) {
        if (query) {
            autoComplete({ variables: { input: { query, types: [EntityType.GlossaryNode] } } });
            setUseSearch(true);
        } else {
            setUseSearch(false);
        }
    }

    function handleUpdate(values: NestedSelectOption[]) {
        if (values.length) {
            const nodeUrnsToUpdate = values.map((v) => v.value);
            onNodesChange(nodeUrnsToUpdate);
        } else {
            onNodesChange([]);
        }
    }

    // Merge options to ensure selected nodes remain visible
    const baseOptions = [...options, ...childOptions].sort((a, b) => a.label.localeCompare(b.label));
    const searchOptions = [...autoCompleteOptions].sort((a, b) => a.label.localeCompare(b.label));

    const defaultOptions = mergeSelectedNestedOptions(baseOptions, initialOptions);
    const searchOptionsWithSelected = mergeSelectedNestedOptions(searchOptions, initialOptions);

    return (
        <NestedSelect
            label={label}
            placeholder={placeholder}
            searchPlaceholder="Search all glossary nodes..."
            options={useSearch ? searchOptionsWithSelected : defaultOptions}
            initialValues={initialOptions}
            loadData={handleLoad}
            onSearch={handleSearch}
            onUpdate={handleUpdate}
            width="full"
            isMultiSelect={isMultiSelect}
            showSearch
            implicitlySelectChildren={false}
            areParentsSelectable
            shouldAlwaysSyncParentValues
            hideParentCheckbox={false}
        />
    );
};

export default GlossaryNodeSelector;
