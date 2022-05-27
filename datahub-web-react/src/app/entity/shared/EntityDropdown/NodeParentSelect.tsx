import React, { useState, useEffect } from 'react';
import { Select } from 'antd';
import { useGetSearchResultsLazyQuery } from '../../../../graphql/search.generated';
import { EntityType, GlossaryNode } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useEntityData } from '../EntityContext';
import ClickOutside from '../../../shared/ClickOutside';
import GlossaryBrowser from '../../../glossary/GlossaryBrowser/GlossaryBrowser';
import { BrowserWrapper } from '../../../shared/tags/AddTagsTermsModal';

// filter out entity itself and its children
export function filterResultsForMove(entity: GlossaryNode, entityUrn: string) {
    return (
        entity.urn !== entityUrn &&
        entity.__typename === 'GlossaryNode' &&
        !entity.parentNodes?.nodes.some((node) => node.urn === entityUrn)
    );
}

interface Props {
    selectedParentUrn: string;
    setSelectedParentUrn: (parent: string) => void;
    isMoving?: boolean;
}

function NodeParentSelect(props: Props) {
    const { selectedParentUrn, setSelectedParentUrn, isMoving } = props;
    const [selectedParentName, setSelectedParentName] = useState('');
    const [isFocusedOnInput, setIsFocusedOnInput] = useState(false);
    const [searchQuery, setSearchQuery] = useState('');
    const entityRegistry = useEntityRegistry();
    const { entityData, urn: entityDataUrn } = useEntityData();

    const [nodeSearch, { data: nodeData }] = useGetSearchResultsLazyQuery();
    let nodeSearchResults = nodeData?.search?.searchResults || [];
    if (isMoving) {
        nodeSearchResults = nodeSearchResults.filter((r) =>
            filterResultsForMove(r.entity as GlossaryNode, entityDataUrn),
        );
    }

    useEffect(() => {
        if (entityData && selectedParentUrn === entityDataUrn) {
            const displayName = entityRegistry.getDisplayName(EntityType.GlossaryNode, entityData);
            setSelectedParentName(displayName);
        }
    }, [entityData, entityRegistry, selectedParentUrn, entityDataUrn]);

    function handleSearch(text: string) {
        setSearchQuery(text);
        nodeSearch({
            variables: {
                input: {
                    type: EntityType.GlossaryNode,
                    query: text,
                    start: 0,
                    count: 5,
                },
            },
        });
    }

    function onSelectParentNode(parentNodeUrn: string) {
        const selectedNode = nodeSearchResults.find((result) => result.entity.urn === parentNodeUrn);
        if (selectedNode) {
            setSelectedParentUrn(parentNodeUrn);
            const displayName = entityRegistry.getDisplayName(selectedNode.entity.type, selectedNode.entity);
            setSelectedParentName(displayName);
        }
    }

    function clearSelectedParent() {
        setSelectedParentUrn('');
        setSelectedParentName('');
        setSearchQuery('');
    }

    function selectNodeFromBrowser(urn: string, displayName: string) {
        setIsFocusedOnInput(false);
        setSelectedParentUrn(urn);
        setSelectedParentName(displayName);
    }

    const isShowingGlossaryBrowser = !searchQuery && isFocusedOnInput;

    return (
        <ClickOutside onClickOutside={() => setIsFocusedOnInput(false)}>
            <Select
                showSearch
                allowClear
                filterOption={false}
                value={selectedParentName}
                onSelect={onSelectParentNode}
                onSearch={handleSearch}
                onClear={clearSelectedParent}
                onFocus={() => setIsFocusedOnInput(true)}
                dropdownStyle={isShowingGlossaryBrowser || !searchQuery ? { display: 'none' } : {}}
            >
                {nodeSearchResults?.map((result) => (
                    <Select.Option key={result?.entity?.urn} value={result.entity.urn}>
                        {entityRegistry.getDisplayName(result.entity.type, result.entity)}
                    </Select.Option>
                ))}
            </Select>
            <BrowserWrapper isHidden={!isShowingGlossaryBrowser}>
                <GlossaryBrowser isSelecting hideTerms selectNode={selectNodeFromBrowser} />
            </BrowserWrapper>
        </ClickOutside>
    );
}

export default NodeParentSelect;
