import React, { useEffect, useMemo, useState, useRef, useCallback, EventHandler, SyntheticEvent } from 'react';
import { Input, AutoComplete, Button } from 'antd';
import { CloseCircleFilled, SearchOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { useHistory } from 'react-router';
import { AutoCompleteResultForEntity, EntityType, FacetFilterInput, ScenarioType } from '../../types.generated';
import EntityRegistry from '../entity/EntityRegistry';
import filterSearchQuery from './utils/filterSearchQuery';
import { ANTD_GRAY, ANTD_GRAY_V2 } from '../entity/shared/constants';
import { getEntityPath } from '../entity/shared/containers/profile/utils';
import { EXACT_SEARCH_PREFIX } from './utils/constants';
import { useListRecommendationsQuery } from '../../graphql/recommendations.generated';
import AutoCompleteItem from './autoComplete/AutoCompleteItem';
import { useQuickFiltersContext } from '../../providers/QuickFiltersContext';
import QuickFilters from './autoComplete/quickFilters/QuickFilters';
import { getFiltersWithQuickFilter } from './utils/filterUtils';
import usePrevious from '../shared/usePrevious';
import analytics, { Event, EventType } from '../analytics';
import RecommendedOption from './autoComplete/RecommendedOption';
import SectionHeader, { EntityTypeLabel } from './autoComplete/SectionHeader';
import { useUserContext } from '../context/useUserContext';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { getQuickFilterDetails } from './autoComplete/quickFilters/utils';
import ViewAllSearchItem from './ViewAllSearchItem';
import { ViewSelect } from '../entity/view/select/ViewSelect';
import { combineSiblingsInAutoComplete } from './utils/combineSiblingsInAutoComplete';

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
        background-color: ${ANTD_GRAY_V2[2]};
    }
    > .ant-input {
        font-size: 14px;
        background-color: ${ANTD_GRAY_V2[2]};
    }
    > .ant-input::placeholder {
        color: ${ANTD_GRAY_V2[10]};
    }
    .ant-input-clear-icon {
        height: 15px;
        width: 15px;
    }
`;

const ClearIcon = styled(CloseCircleFilled)`
    svg {
        height: 15px;
        width: 15px;
    }
`;

const ViewSelectContainer = styled.div`
    &&& {
        border-right: 1px solid ${ANTD_GRAY_V2[5]};
    }
`;

const SearchIcon = styled(SearchOutlined)`
    color: ${ANTD_GRAY_V2[8]};
`;

const EXACT_AUTOCOMPLETE_OPTION_TYPE = 'exact_query';
const RELEVANCE_QUERY_OPTION_TYPE = 'recommendation';

const QUICK_FILTER_AUTO_COMPLETE_OPTION = {
    label: <EntityTypeLabel>Filter by</EntityTypeLabel>,
    options: [
        {
            value: 'quick-filter-unique-key',
            type: '',
            label: <QuickFilters />,
            style: { padding: '8px', cursor: 'auto' },
            disabled: true,
        },
    ],
};

const renderRecommendedQuery = (query: string) => {
    return {
        value: query,
        label: <RecommendedOption text={query} />,
        type: RELEVANCE_QUERY_OPTION_TYPE,
    };
};

const handleStopPropagation: EventHandler<SyntheticEvent> = (e) => {
    e.stopPropagation();
};

interface Props {
    initialQuery?: string;
    placeholderText: string;
    suggestions: Array<AutoCompleteResultForEntity>;
    onSearch: (query: string, type?: EntityType, filters?: FacetFilterInput[]) => void;
    onQueryChange: (query: string) => void;
    style?: React.CSSProperties;
    inputStyle?: React.CSSProperties;
    autoCompleteStyle?: React.CSSProperties;
    entityRegistry: EntityRegistry;
    fixAutoComplete?: boolean;
    hideRecommendations?: boolean;
    showQuickFilters?: boolean;
    viewsEnabled?: boolean;
    combineSiblings?: boolean;
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
    showQuickFilters,
    viewsEnabled = false,
    combineSiblings = false,
    setIsSearchBarFocused,
    onFocus,
    onBlur,
}: Props) => {
    const history = useHistory();
    const [searchQuery, setSearchQuery] = useState<string | undefined>(initialQuery);
    const [selected, setSelected] = useState<string>();
    const [isDropdownVisible, setIsDropdownVisible] = useState(false);
    useEffect(() => setSelected(initialQuery), [initialQuery]);

    const searchEntityTypes = entityRegistry.getSearchEntityTypes();
    const userUrn = useUserContext().user?.urn;

    const { data } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn: userUrn as string,
                requestContext: {
                    scenario: ScenarioType.SearchBar,
                },
                limit: 1,
            },
        },
        skip: hideRecommendations || !userUrn,
    });

    const effectiveQuery = searchQuery !== undefined ? searchQuery : initialQuery || '';

    const onClickExploreAll = useCallback(() => {
        analytics.event({ type: EventType.SearchBarExploreAllClickEvent });
        navigateToSearchUrl({ query: '*', history });
    }, [history]);

    const emptyQueryOptions = useMemo(() => {
        const moduleOptions =
            data?.listRecommendations?.modules.map((module) => ({
                label: <EntityTypeLabel>{module.title}</EntityTypeLabel>,
                options: [...module.content.map((content) => renderRecommendedQuery(content.value))],
            })) || [];

        const exploreAllOption = {
            value: 'explore-all-unique-key',
            type: '',
            label: (
                <Button type="link" onClick={onClickExploreAll}>
                    Explore all →
                </Button>
            ),
            style: { marginLeft: 'auto', cursor: 'auto' },
            disabled: true,
        };

        const tail = showQuickFilters ? [exploreAllOption] : [];

        return [...moduleOptions, ...tail];
    }, [data?.listRecommendations?.modules, onClickExploreAll, showQuickFilters]);

    const { quickFilters, selectedQuickFilter, setSelectedQuickFilter } = useQuickFiltersContext();

    const autoCompleteQueryOptions = useMemo(() => {
        const query = suggestions.length ? effectiveQuery : '';
        const selectedQuickFilterLabel =
            showQuickFilters && selectedQuickFilter
                ? getQuickFilterDetails(selectedQuickFilter, entityRegistry).label
                : '';
        const text = query || selectedQuickFilterLabel;

        if (!text) return [];

        return [
            {
                value: `${EXACT_SEARCH_PREFIX}${text}`,
                label: <ViewAllSearchItem searchTarget={text} />,
                type: EXACT_AUTOCOMPLETE_OPTION_TYPE,
            },
        ];
    }, [showQuickFilters, suggestions.length, effectiveQuery, selectedQuickFilter, entityRegistry]);

    const autoCompleteEntityOptions = useMemo(() => {
        return suggestions.map((suggestion: AutoCompleteResultForEntity) => {
            const combinedSuggestion = combineSiblingsInAutoComplete(suggestion, { combineSiblings });
            return {
                label: <SectionHeader entityType={combinedSuggestion.type} />,
                options: combinedSuggestion.combinedEntities.map((combinedEntity) => ({
                    value: combinedEntity.entity.urn,
                    label: (
                        <AutoCompleteItem
                            query={effectiveQuery}
                            entity={combinedEntity.entity}
                            siblings={combineSiblings ? combinedEntity.matchedEntities : undefined}
                        />
                    ),
                    type: combinedEntity.entity.type,
                    style: { padding: '12px 12px 12px 16px' },
                })),
            };
        });
    }, [combineSiblings, effectiveQuery, suggestions]);

    const previousSelectedQuickFilterValue = usePrevious(selectedQuickFilter?.value);
    useEffect(() => {
        // if we change the selected quick filter, re-issue auto-complete
        if (searchQuery && selectedQuickFilter?.value !== previousSelectedQuickFilterValue) {
            onQueryChange(searchQuery);
        }
    });

    // clear quick filters when this search bar is unmounted (ie. going from search results to home page)
    useEffect(() => {
        return () => {
            setSelectedQuickFilter(null);
        };
    }, [setSelectedQuickFilter]);

    const quickFilterOption = useMemo(() => {
        return showQuickFilters && quickFilters && quickFilters.length > 0 ? [QUICK_FILTER_AUTO_COMPLETE_OPTION] : [];
    }, [quickFilters, showQuickFilters]);

    const options = useMemo(() => {
        // Display recommendations when there is no search query, autocomplete suggestions otherwise.
        const tail = autoCompleteEntityOptions.length ? autoCompleteEntityOptions : emptyQueryOptions;
        return [...quickFilterOption, ...autoCompleteQueryOptions, ...tail];
    }, [emptyQueryOptions, autoCompleteEntityOptions, autoCompleteQueryOptions, quickFilterOption]);

    const searchBarWrapperRef = useRef<HTMLDivElement>(null);

    function handleSearchBarClick(isSearchBarFocused: boolean) {
        if (
            setIsSearchBarFocused &&
            (!isSearchBarFocused ||
                (searchBarWrapperRef && searchBarWrapperRef.current && searchBarWrapperRef.current.clientWidth < 650))
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

    function handleSearch(query: string, type?: EntityType, appliedQuickFilters?: FacetFilterInput[]) {
        onSearch(query, type, appliedQuickFilters);
        if (selectedQuickFilter) {
            setSelectedQuickFilter(null);
        }
    }

    return (
        <AutoCompleteContainer style={style} ref={searchBarWrapperRef}>
            <StyledAutoComplete
                data-testid="search-bar"
                defaultActiveFirstOption={false}
                style={autoCompleteStyle}
                options={options}
                filterOption={false}
                onSelect={(value, option) => {
                    // If the autocomplete option type is NOT an entity, then render as a normal search query.
                    if (option.type === EXACT_AUTOCOMPLETE_OPTION_TYPE || option.type === RELEVANCE_QUERY_OPTION_TYPE) {
                        handleSearch(
                            `${filterSearchQuery(value as string)}`,
                            searchEntityTypes.indexOf(option.type) >= 0 ? option.type : undefined,
                            getFiltersWithQuickFilter(selectedQuickFilter),
                        );
                        analytics.event({
                            type: EventType.SelectAutoCompleteOption,
                            optionType: option.type,
                        } as Event);
                    } else {
                        // Navigate directly to the entity profile.
                        history.push(getEntityPath(option.type, value as string, entityRegistry, false, false));
                        setSelected('');
                        analytics.event({
                            type: EventType.SelectAutoCompleteOption,
                            optionType: option.type,
                            entityType: option.type,
                            entityUrn: value,
                        } as Event);
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
                onDropdownVisibleChange={(isOpen) => {
                    if (!isOpen) {
                        setIsDropdownVisible(isOpen);
                    } else {
                        // set timeout so that we allow search bar to grow in width and therefore allow autocomplete to grow
                        setTimeout(() => {
                            setIsDropdownVisible(isOpen);
                        }, 0);
                    }
                }}
                open={isDropdownVisible}
                listHeight={480}
            >
                <StyledSearchBar
                    bordered={false}
                    placeholder={placeholderText}
                    onPressEnter={() => {
                        handleSearch(
                            filterSearchQuery(searchQuery || ''),
                            undefined,
                            getFiltersWithQuickFilter(selectedQuickFilter),
                        );
                    }}
                    style={{ ...inputStyle, color: 'red' }}
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    data-testid="search-input"
                    onFocus={handleFocus}
                    onBlur={handleBlur}
                    allowClear={{ clearIcon: <ClearIcon /> }}
                    prefix={
                        <>
                            {viewsEnabled && (
                                <ViewSelectContainer
                                    onClick={handleStopPropagation}
                                    onFocus={handleStopPropagation}
                                    onMouseDown={handleStopPropagation}
                                    onKeyUp={handleStopPropagation}
                                    onKeyDown={handleStopPropagation}
                                >
                                    <ViewSelect
                                        dropdownStyle={
                                            fixAutoComplete
                                                ? {
                                                      position: 'fixed',
                                                  }
                                                : {}
                                        }
                                    />
                                </ViewSelectContainer>
                            )}
                            <SearchIcon
                                onClick={() => {
                                    handleSearch(
                                        filterSearchQuery(searchQuery || ''),
                                        undefined,
                                        getFiltersWithQuickFilter(selectedQuickFilter),
                                    );
                                }}
                            />
                        </>
                    }
                />
            </StyledAutoComplete>
        </AutoCompleteContainer>
    );
};

SearchBar.defaultProps = defaultProps;
