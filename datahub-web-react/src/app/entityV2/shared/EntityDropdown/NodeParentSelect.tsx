import { Select } from 'antd';
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import useParentSelector from '@app/entityV2/shared/EntityDropdown/useParentSelector';
import GlossaryBrowser from '@app/glossary/GlossaryBrowser/GlossaryBrowser';
import ClickOutside from '@app/shared/ClickOutside';
import { BrowserWrapper } from '@app/shared/tags/AddTagsTermsModal';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, GlossaryNode } from '@types';

// filter out entity itself and its children
export function filterResultsForMove(entity: GlossaryNode, entityUrn: string) {
    return (
        entity.urn !== entityUrn &&
        entity.__typename === 'GlossaryNode' &&
        !entity.parentNodes?.nodes?.some((node) => node.urn === entityUrn)
    );
}

interface Props {
    selectedParentUrn: string;
    setSelectedParentUrn: (parent: string) => void;
    isMoving?: boolean;
}

function NodeParentSelect(props: Props) {
    const { selectedParentUrn, setSelectedParentUrn, isMoving } = props;
    const entityRegistry = useEntityRegistry();
    const { entityData, urn: entityDataUrn, entityType } = useEntityData();

    const {
        searchResults,
        searchQuery,
        isFocusedOnInput,
        selectedParentName,
        selectParentFromBrowser,
        onSelectParent,
        handleSearch,
        clearSelectedParent,
        setIsFocusedOnInput,
    } = useParentSelector({
        entityType: EntityType.GlossaryNode,
        entityData,
        selectedParentUrn,
        setSelectedParentUrn,
    });

    const nodeSearchResults = isMoving
        ? searchResults.filter((r) => filterResultsForMove(r as GlossaryNode, entityDataUrn))
        : searchResults;

    const isShowingGlossaryBrowser = !searchQuery && isFocusedOnInput;
    const shouldHideSelf = isMoving && entityType === EntityType.GlossaryNode;

    return (
        <ClickOutside onClickOutside={() => setIsFocusedOnInput(false)}>
            <Select
                showSearch
                allowClear
                filterOption={false}
                value={selectedParentName}
                onSelect={onSelectParent}
                onSearch={handleSearch}
                onClear={clearSelectedParent}
                onFocus={() => setIsFocusedOnInput(true)}
                dropdownStyle={isShowingGlossaryBrowser || !searchQuery ? { display: 'none' } : {}}
            >
                {nodeSearchResults?.map((result) => (
                    <Select.Option key={result?.urn} value={result.urn}>
                        {entityRegistry.getDisplayName(result.type, result)}
                    </Select.Option>
                ))}
            </Select>
            <BrowserWrapper isHidden={!isShowingGlossaryBrowser}>
                <GlossaryBrowser
                    isSelecting
                    hideTerms
                    selectNode={selectParentFromBrowser}
                    nodeUrnToHide={shouldHideSelf ? entityData?.urn : undefined}
                />
            </BrowserWrapper>
        </ClickOutside>
    );
}

export default NodeParentSelect;
