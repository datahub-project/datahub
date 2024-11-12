import { useState, useEffect } from 'react';
import { message } from 'antd';
import { useBatchAddTagsMutation, useBatchRemoveTagsMutation } from '@src/graphql/mutations.generated';
import { handleBatchError } from '@src/app/entityV2/shared/utils';
import { CheckboxValueType } from 'antd/lib/checkbox/Group';
import { EntityType, Tag } from '@src/types.generated';
import { useGetAutoCompleteResultsLazyQuery } from '@src/graphql/search.generated';
import { debounce } from 'lodash';

interface UseUpdateTagParams {
    resourceUrn: string;
    selectedTags: any[];
    refetch?: () => void;
    onClose?: () => void;
    tags: any[];
}

type TagOption = { label: string; value: string; tag: Tag };

const MAX_EXISTING_TAG_SEARCH_RESULTS = 2; // Max results for previously added tags
const MAX_TAGS_TO_BE_SEARCHED = 10; // Max tags to be searched when filtering
const DEBOUNCE_DELAY_TO_SEARCH_TAG_MS = 300;

export const useTagOperations = ({ resourceUrn, selectedTags, refetch, tags, onClose }: UseUpdateTagParams) => {
    const [searchText, setSearchText] = useState<string>(''); // Tracks the current search input

    // State for organizing tag options
    const [previouslyAddedOptions, setPreviouslyAddedOptions] = useState<TagOption[]>([]); // Tags already added
    const [selectedTagOptions, setSelectedTagOptions] = useState<CheckboxValueType[]>([]); // User's current selections

    // Filtered tag options based on search results or already added tags
    const [filteredAddableOptions, setFilteredAddableOptions] = useState<TagOption[]>([]);
    const [filteredPreviouslyAddedOptions, setFilteredPreviouslyAddedOptions] = useState<TagOption[]>([]);

    // GraphQL mutations for adding/removing tags
    const [batchAddTagsMutation] = useBatchAddTagsMutation();
    const [batchRemoveTagsMutation] = useBatchRemoveTagsMutation();

    // Lazy query for searching tag terms via GraphQL
    const [tagTermSearch, { data: tagsSearchData, loading: tagsSearchResultsLoading }] =
        useGetAutoCompleteResultsLazyQuery();

    // Handles search input for tag autocomplete
    const handleSearchTags = (e: React.ChangeEvent<HTMLInputElement>) => {
        const { value } = e.target;
        setSearchText(value);
    };

    useEffect(() => {
        if (searchText.length > 0) {
            const debouncedSearch = debounce(() => {
                tagTermSearch({
                    variables: {
                        input: {
                            type: EntityType.Tag,
                            query: searchText,
                            limit: MAX_TAGS_TO_BE_SEARCHED,
                        },
                    },
                });
            }, DEBOUNCE_DELAY_TO_SEARCH_TAG_MS);
            debouncedSearch();

            // Cleanup debounce on unmount or when searchText changes
            return () => debouncedSearch.cancel();
        }
        return undefined; // Explicitly return undefined when searchText is empty
    }, [searchText, tagTermSearch]);

    // Updates tag options based on current search or selected tags
    const setTagOptions = (allTags) => {
        // Convert all tags to option objects
        const allOptions = allTags.map((item) => ({
            label: item?.name,
            value: item?.urn,
            tag: item,
        }));

        const selectedOptions = selectedTags?.map((tagItem) => tagItem?.tag.urn);

        // No search text: Filter based on already selected tags
        if (!searchText?.length) {
            // Tags already selected
            const prevOptions = selectedTags?.map((item) => ({
                label: item?.tag?.name,
                value: item?.tag?.urn,
                tag: item?.tag,
            }));
            setPreviouslyAddedOptions(prevOptions);
            setFilteredPreviouslyAddedOptions(prevOptions);

            // Tags available to add
            const otherTags = tags.filter((tag) => !selectedOptions?.includes(tag.urn));
            const otherOptions = otherTags.map((item) => ({
                label: item?.name,
                value: item?.urn,
                tag: item,
            }));
            setFilteredAddableOptions(otherOptions);
        } else {
            // Search text present: Filter options based on the search

            // Tags already selected, limited to max results
            const prevOptions = allOptions.filter((option) => selectedOptions?.includes(option.value));
            setFilteredPreviouslyAddedOptions(prevOptions.slice(0, MAX_EXISTING_TAG_SEARCH_RESULTS));

            // Tags available to add, excluding already selected
            const otherOptions = allOptions.filter((option) => !selectedOptions?.includes(option.value));
            setFilteredAddableOptions(otherOptions);
        }
    };

    // Updates selected tags when `selectedTags` changes
    useEffect(() => {
        const selectedOptions = selectedTags?.map((tagItem) => tagItem?.tag.urn);
        setSelectedTagOptions(selectedOptions);
    }, [selectedTags]);

    // Updates tag options based on search results or tag list
    useEffect(() => {
        const tagSearchResults = tagsSearchData?.autoComplete?.entities || [];
        const allTags = searchText ? tagSearchResults : tags;
        setTagOptions(allTags);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [searchText, tagsSearchData]);

    // Updates tag options when `tags` or `selectedTags` changes
    useEffect(() => {
        if (tags.length) {
            setTagOptions(tags);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [selectedTags, tags]);

    // Adds multiple tags to the resource
    const batchAddTags = async (newTags: string[]) => {
        try {
            await batchAddTagsMutation({
                variables: {
                    input: {
                        tagUrns: newTags,
                        resources: [{ resourceUrn }],
                    },
                },
            });
        } catch (e) {
            message.error(handleBatchError(newTags, e, 'Failed to add tags.'));
        }
    };

    // Removes multiple tags from the resource
    const batchRemoveTags = async (removedTags: string[]) => {
        try {
            await batchRemoveTagsMutation({
                variables: {
                    input: {
                        tagUrns: removedTags,
                        resources: [{ resourceUrn }],
                    },
                },
            });
        } catch (e) {
            message.error(handleBatchError(removedTags, e, 'Failed to remove tags.'));
        }
    };

    // Handles updating tags (add and remove)
    const handleUpdateTags = async ({ isRemoveAll }: { isRemoveAll?: boolean }) => {
        const olderSelectedTags = selectedTags?.map((tagItem) => tagItem?.tag.urn);
        // if isRemoveAll is true then remove all the older selected tag
        const removedTags = isRemoveAll
            ? olderSelectedTags
            : olderSelectedTags?.filter((tag) => !selectedTagOptions?.includes(tag));

        // if isRemoveAll is true then remove all the newly selected tag
        const newlySelectedTags = isRemoveAll
            ? []
            : selectedTagOptions.filter((tag) => !olderSelectedTags?.includes(tag));

        if (newlySelectedTags?.length || removedTags?.length) {
            try {
                // Add new tags
                if (newlySelectedTags?.length) {
                    await batchAddTags(newlySelectedTags as string[]);
                }
                // Remove unselected tags
                if (removedTags?.length) {
                    await batchRemoveTags(removedTags as string[]);
                }

                // Notify success and refresh UI
                message.success('Tags Updated!', 2);
                onClose?.();
                refetch?.();
                setSearchText('');
            } catch (e) {
                console.log(e);
            }
        }
    };

    return {
        filteredAddableOptions,
        filteredPreviouslyAddedOptions,
        previouslyAddedOptions,
        selectedTagOptions,
        setSelectedTagOptions,
        handleUpdateTags,
        searchText,
        handleSearchTags,
        tagsSearchResultsLoading,
        tagsSearchData,
    };
};
