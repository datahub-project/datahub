import React, { useEffect, useMemo, useState, useRef } from 'react';
import { Input, AutoComplete, Typography } from 'antd';
import { SearchOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { useHistory } from 'react-router';
import { AutoCompleteResultForEntity, Entity, EntityType, ScenarioType } from '../../types.generated';
import EntityRegistry from '../entity/EntityRegistry';
import filterSearchQuery from './utils/filterSearchQuery';
import { ANTD_GRAY } from '../entity/shared/constants';
import { getEntityPath } from '../entity/shared/containers/profile/utils';
import { EXACT_SEARCH_PREFIX } from './utils/constants';
import { useListRecommendationsQuery } from '../../graphql/recommendations.generated';
import { useGetAuthenticatedUserUrn } from '../useGetAuthenticatedUser';
import AutoCompleteItem, { SuggestionContainer } from './autoComplete/AutoCompleteItem';

const ExploreForEntity = styled.span`
    font-weight: light;
    font-size: 11px;
`;

const StyledAutoComplete = styled(AutoComplete)`
    width: 100%;
    max-width: 650px;
`;

const AutoCompleteContainer = styled.div`
    width: 100%;
    padding: 0 30px;
`;

const StyledSearchBar = styled(Input)`
    &&& {
        border-radius: 70px;
        height: 40px;
        font-size: 20px;
        color: ${ANTD_GRAY[7]};
    }
    > .ant-input {
        font-size: 14px;
    }
`;

const EXACT_AUTOCOMPLETE_OPTION_TYPE = 'exact_query';
const RECOMMENDED_QUERY_OPTION_TYPE = 'recommendation';

const renderItem = (query: string, entity: Entity) => {
    return {
        value: entity.urn,
        label: <AutoCompleteItem query={query} entity={entity} />,
        type: entity.type,
        style: { paddingLeft: 16 },
    };
};

const renderRecommendedQuery = (query: string) => {
    return {
        value: query,
        label: query,
        type: RECOMMENDED_QUERY_OPTION_TYPE,
    };
};

interface Props {
    initialQuery?: string;
    placeholderText: string;
    suggestions: Array<AutoCompleteResultForEntity>;
    onSearch: (query: string, type?: EntityType) => void;
    onQueryChange: (query: string) => void;
    style?: React.CSSProperties;
    inputStyle?: React.CSSProperties;
    autoCompleteStyle?: React.CSSProperties;
    entityRegistry: EntityRegistry;
    fixAutoComplete?: boolean;
    hideRecommendations?: boolean;
    setIsSearchBarFocused?: (isSearchBarFocused: boolean) => void;
    onFocus?: () => void;
    onBlur?: () => void;
}

const defaultProps = {
    style: undefined,
};

/**
 * Represents the search bar appearing in the default header view.
 */
export const SearchBar = ({
    initialQuery,
    placeholderText,
    suggestions,
    onSearch,
    onQueryChange,
    entityRegistry,
    style,
    inputStyle,
    autoCompleteStyle,
    fixAutoComplete,
    hideRecommendations,
    setIsSearchBarFocused,
    onFocus,
    onBlur,
}: Props) => {
    const history = useHistory();
    const [searchQuery, setSearchQuery] = useState<string>();
    const [selected, setSelected] = useState<string>();
    useEffect(() => setSelected(initialQuery), [initialQuery]);

    const searchEntityTypes = entityRegistry.getSearchEntityTypes();
    const userUrn = useGetAuthenticatedUserUrn();

    const { data } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn,
                requestContext: {
                    scenario: ScenarioType.SearchBar,
                },
                limit: 1,
            },
        },
        skip: hideRecommendations,
    });

    const effectiveQuery = searchQuery !== undefined ? searchQuery : initialQuery || '';

    const emptyQueryOptions = useMemo(() => {
        // Map each module to a set of
        return (
            data?.listRecommendations?.modules.map((module) => ({
                label: module.title,
                options: [...module.content.map((content) => renderRecommendedQuery(content.value))],
            })) || []
        );
    }, [data]);

    const autoCompleteQueryOptions = useMemo(
        () =>
            (suggestions?.length > 0 &&
                effectiveQuery.length > 0 && [
                    {
                        value: `${EXACT_SEARCH_PREFIX}${effectiveQuery}`,
                        label: (
                            <SuggestionContainer key={EXACT_AUTOCOMPLETE_OPTION_TYPE}>
                                <ExploreForEntity>
                                    View all results for <Typography.Text strong>{effectiveQuery}</Typography.Text>
                                </ExploreForEntity>
                            </SuggestionContainer>
                        ),
                        type: EXACT_AUTOCOMPLETE_OPTION_TYPE,
                    },
                ]) ||
            [],
        [suggestions, effectiveQuery],
    );

    const autoCompleteEntityOptions = useMemo(
        () =>
            suggestions.map((entity: AutoCompleteResultForEntity) => ({
                label: entityRegistry.getCollectionName(entity.type),
                options: [...entity.entities.map((e: Entity) => renderItem(effectiveQuery, e))],
            })),
        [effectiveQuery, suggestions, entityRegistry],
    );

    const options = useMemo(() => {
        // Display recommendations when there is no search query, autocomplete suggestions otherwise.
        if (autoCompleteEntityOptions.length > 0) {
            return [...autoCompleteQueryOptions, ...autoCompleteEntityOptions];
        }
        return emptyQueryOptions;
    }, [emptyQueryOptions, autoCompleteEntityOptions, autoCompleteQueryOptions]);

    const searchBarWrapperRef = useRef<HTMLDivElement>(null);

    function handleSearchBarClick(isSearchBarFocused: boolean) {
        if (
            setIsSearchBarFocused &&
            (!isSearchBarFocused ||
                (searchBarWrapperRef && searchBarWrapperRef.current && searchBarWrapperRef.current.clientWidth < 590))
        ) {
            setIsSearchBarFocused(isSearchBarFocused);
        }
    }

    function handleFocus() {
        if (onFocus) onFocus();
        handleSearchBarClick(true);
    }

    function handleBlur() {
        if (onBlur) onBlur();
        handleSearchBarClick(false);
    }

    return (
        <AutoCompleteContainer style={style} ref={searchBarWrapperRef}>
            <StyledAutoComplete
                defaultActiveFirstOption={false}
                style={autoCompleteStyle}
                options={options}
                filterOption={false}
                onSelect={(value, option) => {
                    // If the autocomplete option type is NOT an entity, then render as a normal search query.
                    if (
                        option.type === EXACT_AUTOCOMPLETE_OPTION_TYPE ||
                        option.type === RECOMMENDED_QUERY_OPTION_TYPE
                    ) {
                        onSearch(
                            `${filterSearchQuery(value as string)}`,
                            searchEntityTypes.indexOf(option.type) >= 0 ? option.type : undefined,
                        );
                    } else {
                        // Navigate directly to the entity profile.
                        history.push(getEntityPath(option.type, value as string, entityRegistry, false, false));
                        setSelected('');
                    }
                }}
                onSearch={(value: string) => onQueryChange(value)}
                defaultValue={initialQuery || undefined}
                value={selected}
                onChange={(v) => setSelected(filterSearchQuery(v as string))}
                dropdownStyle={{
                    maxHeight: 1000,
                    overflowY: 'visible',
                    position: (fixAutoComplete && 'fixed') || 'relative',
                }}
            >
                <StyledSearchBar
                    placeholder={placeholderText}
                    onPressEnter={() => {
                        // e.stopPropagation();
                        onSearch(filterSearchQuery(searchQuery || ''));
                    }}
                    style={inputStyle}
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    data-testid="search-input"
                    onFocus={handleFocus}
                    onBlur={handleBlur}
                    allowClear
                    prefix={<SearchOutlined onClick={() => onSearch(filterSearchQuery(searchQuery || ''))} />}
                />
            </StyledAutoComplete>
        </AutoCompleteContainer>
    );
};

SearchBar.defaultProps = defaultProps;
