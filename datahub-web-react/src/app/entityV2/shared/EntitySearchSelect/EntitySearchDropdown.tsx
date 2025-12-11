import { LoadingOutlined } from '@ant-design/icons';
import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import styled from 'styled-components';

import Dropdown from '@components/components/Dropdown/Dropdown';
import { Input } from '@components/components/Input/Input';
import {
    DropdownContainer,
    LabelContainer,
    OptionLabel,
    OptionList,
    StyledCheckbox,
} from '@components/components/Select/components';

import EntitySearchInputResultV2 from '@app/entityV2/shared/EntitySearchInput/EntitySearchInputResultV2';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetSearchResultsForMultipleLazyQuery } from '@graphql/search.generated';
import { AndFilterInput, Entity, EntityType } from '@types';

const SearchInputContainer = styled.div({
    position: 'relative',
    width: '100%',
    display: 'flex',
    justifyContent: 'center',
});

const EntityOptionContainer = styled.div`
    width: 100%;
    flex: 1;
`;

const LoadingState = styled.div`
    padding: 16px 12px;
    text-align: center;
    color: #8c8c8c;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
`;

const EmptyState = styled.div`
    padding: 16px 12px;
    text-align: center;
    color: #8c8c8c;
    font-style: italic;
`;

export interface EntitySearchDropdownProps {
    entityTypes: EntityType[];
    selectedUrns: string[];
    onSelectionChange: (urns: string[]) => void;
    placeholder?: string;
    isMultiSelect?: boolean;
    onEntitySelect?: (entity: Entity) => void; // Called when an entity is selected (for caching)
    defaultFilters?: AndFilterInput[]; // Default filters to apply to search (e.g., filter out unpublished documents)
    viewUrn?: string; // Optional view URN to apply to search
    // Dropdown configuration
    trigger: React.ReactNode; // The element that triggers the dropdown
    open: boolean; // Controlled open state
    onOpenChange: (open: boolean) => void; // Callback when open state changes
    placement?: 'top' | 'bottom' | 'bottomLeft' | 'bottomRight' | 'topLeft' | 'topRight'; // Dropdown placement
    disabled?: boolean; // Whether dropdown is disabled
    actionButtons?: React.ReactNode; // Optional action buttons to render below the search content
    dropdownContainerStyle?: React.CSSProperties; // Optional custom styles for dropdown container
}

/**
 * A reusable component that renders a dropdown for entity search.
 * Handles all search logic internally and provides a clean interface.
 * Includes the Dropdown wrapper, so consumers don't need to wrap it themselves.
 */
export const EntitySearchDropdown: React.FC<EntitySearchDropdownProps> = ({
    entityTypes,
    selectedUrns,
    onSelectionChange,
    placeholder = 'Search for entities...',
    isMultiSelect = true,
    onEntitySelect,
    defaultFilters,
    viewUrn,
    trigger,
    open,
    onOpenChange,
    placement = 'bottomRight',
    disabled = false,
    actionButtons,
    dropdownContainerStyle,
}) => {
    const entityRegistry = useEntityRegistry();
    const [searchQuery, setSearchQuery] = useState('');
    const prevOpenRef = useRef<boolean>(false);
    const dropdownRef = useRef<HTMLDivElement>(null);

    // Search functionality
    const [searchResources, { data: resourcesSearchData, loading: searchLoading }] =
        useGetSearchResultsForMultipleLazyQuery();

    // Issue a default search when dropdown opens
    useEffect(() => {
        if (open && !prevOpenRef.current) {
            searchResources({
                variables: {
                    input: {
                        types: entityTypes,
                        query: '*',
                        start: 0,
                        count: 10,
                        orFilters: defaultFilters,
                        viewUrn: viewUrn || undefined,
                    },
                },
            });
            setSearchQuery('');
        }
        prevOpenRef.current = open;
    }, [open, entityTypes, searchResources, defaultFilters, viewUrn]);

    const handleSearchChange = useCallback(
        (value: string) => {
            setSearchQuery(value);
            searchResources({
                variables: {
                    input: {
                        types: entityTypes,
                        query: value || '*',
                        start: 0,
                        count: 10,
                        orFilters: defaultFilters,
                        viewUrn: viewUrn || undefined,
                    },
                },
            });
        },
        [entityTypes, searchResources, defaultFilters, viewUrn],
    );

    const entityOptions = useMemo(() => {
        const results = resourcesSearchData?.searchAcrossEntities?.searchResults || [];
        return results.map((result) => ({
            label: entityRegistry.getDisplayName(result.entity.type, result.entity),
            value: result.entity.urn,
            entity: result.entity as Entity,
        }));
    }, [resourcesSearchData, entityRegistry]);

    const handleOptionClick = useCallback(
        (option: { value: string; entity: Entity }) => {
            // Notify parent about entity selection (for caching)
            onEntitySelect?.(option.entity);

            if (isMultiSelect) {
                const newUrns = selectedUrns.includes(option.value)
                    ? selectedUrns.filter((u) => u !== option.value)
                    : [...selectedUrns, option.value];
                onSelectionChange(newUrns);
            } else {
                // Single select - just set the one value
                onSelectionChange([option.value]);
            }
        },
        [selectedUrns, onSelectionChange, isMultiSelect, onEntitySelect],
    );

    const dropdownContent = (
        <DropdownContainer ref={dropdownRef} style={dropdownContainerStyle}>
            <SearchInputContainer>
                <Input
                    label=""
                    value={searchQuery}
                    setValue={handleSearchChange}
                    placeholder={placeholder}
                    icon={{ icon: 'Search' }}
                    data-testid="entity-search-select-input"
                />
            </SearchInputContainer>
            <OptionList>
                {searchLoading && (
                    <LoadingState>
                        <LoadingOutlined />
                    </LoadingState>
                )}
                {!searchLoading && entityOptions.length === 0 && <EmptyState>No entities found</EmptyState>}
                {!searchLoading &&
                    entityOptions.map((option) => (
                        <OptionLabel
                            key={option.value}
                            onClick={(e) => {
                                e.preventDefault();
                                e.stopPropagation();
                                handleOptionClick(option);
                            }}
                            onMouseDown={(e) => {
                                // Prevent default to avoid any label/checkbox default behavior
                                e.preventDefault();
                            }}
                            isSelected={selectedUrns.includes(option.value)}
                            isMultiSelect={isMultiSelect}
                            data-testid={`entity-search-option-${option.entity.urn.split(':').pop()}`}
                        >
                            {isMultiSelect ? (
                                <LabelContainer>
                                    <EntityOptionContainer>
                                        <EntitySearchInputResultV2 entity={option.entity} />
                                    </EntityOptionContainer>
                                    <StyledCheckbox
                                        onClick={(e) => {
                                            e.stopPropagation(); // Prevent double-triggering
                                            handleOptionClick(option);
                                        }}
                                        checked={selectedUrns.includes(option.value)}
                                    />
                                </LabelContainer>
                            ) : (
                                <EntityOptionContainer>
                                    <EntitySearchInputResultV2 entity={option.entity} />
                                </EntityOptionContainer>
                            )}
                        </OptionLabel>
                    ))}
            </OptionList>
            {actionButtons}
        </DropdownContainer>
    );

    return (
        <Dropdown
            trigger={['click']}
            open={open}
            onOpenChange={onOpenChange}
            placement={placement}
            disabled={disabled}
            dropdownRender={() => dropdownContent}
        >
            {trigger}
        </Dropdown>
    );
};
