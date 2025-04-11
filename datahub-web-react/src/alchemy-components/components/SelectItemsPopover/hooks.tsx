import { useState, useEffect } from 'react';
import { EntityType } from '@src/types.generated';
import { useGetAutoCompleteResultsLazyQuery } from '@src/graphql/search.generated';
import { debounce } from 'lodash';
import { useEntityRegistry } from '@src/app/useEntityRegistry';

interface UseUpdateEntityParams {
    selectedItems: any[];
    entityType: EntityType;
    refetch?: () => void;
    onClose?: () => void;
    entities: any[];
    searchLimit?: number;
    handleSelectionChange: ({
        selectedItems,
        removedItems,
    }: {
        selectedItems: string[];
        removedItems: string[];
    }) => void;
}

type SelectOption = { label: string; value: string; item: any };

const MAX_EXISTING_SEARCH_RESULTS = 2; // Max results for previously added entities
const DEBOUNCE_DELAY_TO_SEARCH_ENTITY_MS = 300;

export const useEntityOperations = ({
    selectedItems,
    entities,
    searchLimit = 10,
    entityType,
    handleSelectionChange,
}: UseUpdateEntityParams) => {
    const [searchText, setSearchText] = useState<string>(''); // Tracks the current search input
    const entityRegistry = useEntityRegistry();

    // State for organizing entity options
    const [previouslyAddedOptions, setPreviouslyAddedOptions] = useState<SelectOption[]>([]); // Entities already added
    const [selectedOptions, setSelectedOptions] = useState<string[]>([]); // User's current selections

    // Filtered entity options based on search results or already added entities
    const [filteredAddableOptions, setFilteredAddableOptions] = useState<SelectOption[]>([]);
    const [filteredPreviouslyAddedOptions, setFilteredPreviouslyAddedOptions] = useState<SelectOption[]>([]);

    // Lazy query for searching entity terms via GraphQL
    const [entitySearch, { data: searchData, loading: entitySearchResultsLoading }] =
        useGetAutoCompleteResultsLazyQuery();

    // Handles search input for entity autocomplete
    const handleSearchEntities = (e: React.ChangeEvent<HTMLInputElement>) => {
        const { value } = e.target;
        setSearchText(value);
    };

    useEffect(() => {
        if (searchText.length > 0) {
            const debouncedSearch = debounce(() => {
                entitySearch({
                    variables: {
                        input: {
                            type: entityType,
                            query: searchText,
                            limit: searchLimit,
                        },
                    },
                });
            }, DEBOUNCE_DELAY_TO_SEARCH_ENTITY_MS);
            debouncedSearch();

            // Cleanup debounce on unmount or when searchText changes
            return () => debouncedSearch.cancel();
        }
        return undefined; // Explicitly return undefined when searchText is empty
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [searchText, entitySearch]);

    const getOptionsFromEntities = (allEntities) => {
        return allEntities?.map((entity) => ({
            label: entityRegistry.getEntityName(entity.type),
            value: entity?.urn,
            item: entity,
        }));
    };

    // Updates entity options based on current search or selected entities
    const assignEntitiesOptions = (allEntities) => {
        // Convert all entities to option objects
        const allOptions = getOptionsFromEntities(allEntities);

        const selectedEntityOptions = selectedItems?.map((item) => item.urn);

        // No search text: Filter based on already selected entities
        if (!searchText?.length) {
            // Entities already selected
            const prevOptions = getOptionsFromEntities(selectedItems);
            setPreviouslyAddedOptions(prevOptions);
            setFilteredPreviouslyAddedOptions(prevOptions);

            // Entities available to add
            const otherEntities = entities.filter((item) => !selectedEntityOptions?.includes(item?.urn));
            const otherOptions = getOptionsFromEntities(otherEntities);
            setFilteredAddableOptions(otherOptions);
        } else {
            // Search text present: Filter options based on the search

            // Entities already selected, limited to max results
            const prevOptions = allOptions.filter((option) => selectedEntityOptions?.includes(option.value));
            setFilteredPreviouslyAddedOptions(prevOptions.slice(0, MAX_EXISTING_SEARCH_RESULTS));

            // Entities available to add, excluding already selected
            const otherOptions = allOptions.filter((option) => !selectedEntityOptions?.includes(option.value));
            setFilteredAddableOptions(otherOptions);
        }
    };

    // Updates selected entities when `selectedItems` changes
    useEffect(() => {
        const selectedEntityOptions = selectedItems?.map((item) => item.urn);
        setSelectedOptions(selectedEntityOptions);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedItems]);

    // Updates entity options based on search results or entity list
    useEffect(() => {
        const searchResults = searchData?.autoComplete?.entities || [];
        const allEntities = searchText ? searchResults : entities;
        assignEntitiesOptions(allEntities);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [searchText, searchData]);

    // Updates entity options when `entities` or `selectedItems` changes
    useEffect(() => {
        if (entities.length) {
            assignEntitiesOptions(entities);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedItems, entities]);

    // Handles updating entities (add and remove)
    const handleUpdate = async ({ isRemoveAll }: { isRemoveAll?: boolean }) => {
        const olderSelectedEntities = selectedItems?.map((item) => item.urn);
        // if isRemoveAll is true then remove all the older selected entity
        const removedItems = isRemoveAll
            ? olderSelectedEntities
            : olderSelectedEntities?.filter((entity) => !selectedOptions?.includes(entity));

        // if isRemoveAll is true then remove all the newly selected entity
        const addedItems = isRemoveAll
            ? []
            : selectedOptions.filter((entity) => !olderSelectedEntities?.includes(entity));
        handleSelectionChange({ selectedItems: addedItems, removedItems });
        setSearchText('');
    };

    return {
        filteredAddableOptions,
        filteredPreviouslyAddedOptions,
        previouslyAddedOptions,
        selectedOptions,
        setSelectedOptions,
        handleUpdate,
        searchText,
        handleSearchEntities,
        entitySearchResultsLoading,
        searchData,
    };
};
