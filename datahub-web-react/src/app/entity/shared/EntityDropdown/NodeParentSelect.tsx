import React from 'react';
import { Select } from 'antd';
import styled from 'styled-components';
import { EntityType, GlossaryNode, SearchResult } from '../../../../types.generated';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { useEntityData } from '../EntityContext';
import ClickOutside from '../../../shared/ClickOutside';
import GlossaryBrowser from '../../../glossary/GlossaryBrowser/GlossaryBrowser';
import { BrowserWrapper } from '../../../shared/tags/AddTagsTermsModal';
import useParentSelector from './useParentSelector';
import ParentEntities from '../../../search/filters/ParentEntities';
import { getParentGlossary } from '../../../glossary/utils';

const SearchResultContainer = styled.div`
    display: flex;
    flex-direction: column;
    justify-content: center;
`;

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
    autofocus?: boolean;
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

    const nodeSearchResults: SearchResult[] = searchResults.filter((r) =>
        filterResultsForMove(r.entity as GlossaryNode, entityDataUrn),
    );

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
                autoFocus={props.autofocus}
            >
                {nodeSearchResults?.map((result) => (
                    <Select.Option key={result?.entity?.urn} value={result.entity.urn}>
                        <SearchResultContainer>
                            <ParentEntities parentEntities={getParentGlossary(result.entity, entityRegistry)} />
                            {entityRegistry.getDisplayName(result.entity.type, result.entity)}
                        </SearchResultContainer>
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
