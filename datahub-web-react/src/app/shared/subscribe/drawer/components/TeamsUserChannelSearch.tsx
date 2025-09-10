import React, { useEffect, useMemo, useState } from 'react';

import { TeamsSearchResult, useTeamsSearch } from '@app/shared/subscribe/drawer/teams-search-client';
import { SimpleSelect } from '@src/alchemy-components';

type SearchType = 'user' | 'channel';
type SupportedTypes = 'users' | 'channels' | 'both';

type TeamsUserChannelSearchProps = {
    onSelectResult: (result: TeamsSearchResult) => void;
    selectedResult?: TeamsSearchResult | null;
    supportedTypes?: SupportedTypes;
    placeholder?: string;
    disabled?: boolean;
};

export default function TeamsUserChannelSearch({
    onSelectResult,
    selectedResult,
    supportedTypes = 'both',
    placeholder,
    disabled = false,
}: TeamsUserChannelSearchProps) {
    const { searchUsers, searchChannels, channels, users, clearResults } = useTeamsSearch();
    const [isSearching, setIsSearching] = useState(false);

    // Determine what type to search based on supportedTypes
    const searchType: SearchType = useMemo(() => {
        if (supportedTypes === 'users') return 'user';
        if (supportedTypes === 'channels') return 'channel';
        return 'channel'; // Default to channel for 'both' case
    }, [supportedTypes]);

    // Clear search results when component mounts or type changes
    useEffect(() => {
        clearResults();
        setIsSearching(false);
    }, [searchType, clearResults]);

    const handleSearch = (query: string) => {
        if (!query.trim()) {
            clearResults();
            setIsSearching(false);
            return;
        }

        setIsSearching(true);
        if (searchType === 'user') {
            searchUsers(query);
        } else {
            searchChannels(query);
        }
    };

    const handleSelectOption = (values: string[]) => {
        if (values.length === 0) {
            setIsSearching(true);

            return;
        }

        const allResults = [...users, ...channels];
        const selectedOption = allResults.find((result) => `${result.type}-${result.id}` === values[0]);

        if (selectedOption) {
            onSelectResult(selectedOption);
            clearResults();
            setIsSearching(false);
        } else {
            setIsSearching(true);
        }
    };

    const renderSearchOption = (result: TeamsSearchResult) => ({
        value: `${result.type}-${result.id}`,
        label: result.displayName,
        description: result.type === 'user' ? (result as any).email : `${(result as any).teamName} team`,
    });

    const searchOptions = useMemo(() => {
        const searchResults = (searchType === 'user' ? users : channels).map(renderSearchOption);
        // Only include selectedResult as an option if we're not actively searching and there are no other results
        if (selectedResult && searchResults.length === 0 && selectedResult.type === searchType && !isSearching) {
            return [renderSearchOption(selectedResult)];
        }
        return searchResults;
    }, [selectedResult, searchType, users, channels, isSearching]);

    const selectedOptionValues = useMemo(() => {
        // Only show selected values when we have search results or when not actively searching
        if (isSearching) {
            return []; // Don't show selection when actively searching with no results
        }
        if (selectedResult?.type !== searchType) {
            return [];
        }
        return selectedResult ? [`${selectedResult.type}-${selectedResult.id}`] : [];
    }, [selectedResult, isSearching, searchType]);

    const searchPlaceholder = placeholder || `Search for ${searchType === 'user' ? 'team members' : 'channels'}...`;

    return (
        <>
            <SimpleSelect
                width="full"
                showSearch
                placeholder={searchPlaceholder}
                onSearchChange={handleSearch}
                onUpdate={handleSelectOption}
                isDisabled={disabled}
                options={searchOptions}
                values={selectedOptionValues} // Use current selection values
                optionListStyle={{ width: '100%' }}
                emptyState={
                    <div style={{ padding: '8px 0', textAlign: 'center' }}>
                        <div style={{ color: '#8c8c8c', fontSize: '14px', fontWeight: 500 }}>No results found</div>
                        <div style={{ color: '#bfbfbf', fontSize: '12px', marginTop: '4px' }}>
                            No {searchType === 'user' ? 'users' : 'channels'} match your search
                        </div>
                    </div>
                }
            />
        </>
    );
}
