import React, { useEffect, useMemo, useState, useRef, useCallback } from 'react';
import { Input, AutoComplete, Button, Skeleton } from 'antd';
import { CloseCircleFilled, SearchOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { useHistory } from 'react-router';
import { colors } from '@src/alchemy-components';
import { AutoCompleteResultForEntity, FacetFilterInput, ScenarioType } from '../../types.generated';
import { EntityRegistry } from '../../entityRegistryContext';
import filterSearchQuery from './utils/filterSearchQuery';
import { ANTD_GRAY_V2 } from '../entity/shared/constants';
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
import ViewAllSearchItem from './ViewAllSearchItem';
import { ViewSelect } from '../entityV2/view/select/ViewSelect';
import { combineSiblingsInAutoComplete } from './utils/combineSiblingsInAutoComplete';
import { CommandK } from './CommandK';
import { V2_SEARCH_BAR_VIEWS } from '../onboarding/configV2/HomePageOnboardingConfig';
import { REDESIGN_COLORS } from '../entityV2/shared/constants';
import useSearchViewAll from './useSearchViewAll';
import { useAppConfig, useIsShowSeparateSiblingsEnabled } from '../useAppConfig';

const StyledAutoComplete = styled(AutoComplete)<{ $isShowNavBarRedesign?: boolean }>`
    width: 100%;
    max-width: ${(props) => (props.$isShowNavBarRedesign ? '423px' : '540px')};
`;

const SkeletonContainer = styled.div`
    height: 40px;
    width: 100%;
    max-width: 620px;
`;

const SkeletonButton = styled(Skeleton.Button)`
    &&& {
        height: inherit;
        width: inherit;
    }
`;

const AutoCompleteContainer = styled.div<{ viewsEnabled?: boolean; $isShowNavBarRedesign?: boolean }>`
    padding: 0 30px;
    align-items: center;
    border: ${(props) => (props.$isShowNavBarRedesign ? `2px solid ${colors.gray[100]}` : '2px solid transparent')};
    ${(props) => props.$isShowNavBarRedesign && 'box-shadow: 0px 1px 2px 0px rgba(33, 23, 95, 0.07)'};

    transition: border-color 0.3s ease;

    ${(props) =>
        props.viewsEnabled &&
        `
        border-radius: 8px;
        &:focus-within {
            border-color: ${props.$isShowNavBarRedesign ? colors.violet[500] : props.theme.styles['primary-color']};
        }
    `}
`;

const StyledSearchBar = styled(Input)<{
    $textColor?: string;
    $placeholderColor?: string;
    viewsEnabled?: boolean;
    $isShowNavBarRedesign?: boolean;
}>`
    &&& {
        border-radius: 8px;
        height: 40px;
        font-size: 14px;
        color: #dcdcdc;
        background-color: ${ANTD_GRAY_V2[2]};
        border: 2px solid transparent;
        padding-right: 2.5px;
        ${(props) =>
            !props.viewsEnabled &&
            `
        &:focus-within {
            border-color: ${props.theme.styles['primary-color']};
        }`}
    }

    > .ant-input::placeholder {
        color: ${(props) =>
            props.$placeholderColor || (props.$isShowNavBarRedesign ? REDESIGN_COLORS.GREY_300 : '#dcdcdc')};
    }

    > .ant-input {
        color: ${(props) => props.$textColor || (props.$isShowNavBarRedesign ? '#000' : '#fff')};
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
    color: #fff;
    line-height: 20px;
    padding-right: 5.6px;

    &&& {
        border-left: 0px solid ${ANTD_GRAY_V2[5]};
    }
`;

const SearchIcon = styled(SearchOutlined)<{ $isShowNavBarRedesign?: boolean }>`
    color: ${(props) => (props.$isShowNavBarRedesign ? colors.gray[1800] : '#dcdcdc')};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        && svg {
            width: 16px;
            height: 16px;
        }    
    `}
`;

const EXACT_AUTOCOMPLETE_OPTION_TYPE = 'exact_query';
const RELEVANCE_QUERY_OPTION_TYPE = 'recommendation';

const renderRecommendedQuery = (query: string) => {
    return {
        value: query,
        label: <RecommendedOption text={query} />,
        type: RELEVANCE_QUERY_OPTION_TYPE,
    };
};

interface Props {
    id?: string;
    isLoading?: boolean;
    initialQuery?: string;
    placeholderText: string;
    suggestions: Array<AutoCompleteResultForEntity>;
    onSearch: (query: string, filters?: FacetFilterInput[]) => void;
    onQueryChange?: (query: string) => void;
    style?: React.CSSProperties;
    inputStyle?: React.CSSProperties;
    autoCompleteStyle?: React.CSSProperties;
    entityRegistry: EntityRegistry;
    fixAutoComplete?: boolean;
    hideRecommendations?: boolean;
    showQuickFilters?: boolean;
    showCommandK?: boolean;
    viewsEnabled?: boolean;
    combineSiblings?: boolean;
    setIsSearchBarFocused?: (isSearchBarFocused: boolean) => void;
    onFocus?: () => void;
    onBlur?: () => void;
    showViewAllResults?: boolean;
    textColor?: string;
    placeholderColor?: string;
    isShowNavBarRedesign?: boolean;
}

const defaultProps = {
    style: undefined,
};

/**
 * Represents the search bar appearing in the default header view.
 */
export const SearchBar = ({
    id,
    isLoading,
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
    showCommandK = false,
    viewsEnabled = false,
    combineSiblings = false,
    setIsSearchBarFocused,
    onFocus,
    onBlur,
    showViewAllResults = false,
    textColor,
    placeholderColor,
    isShowNavBarRedesign,
}: Props) => {
    const history = useHistory();
    const [searchQuery, setSearchQuery] = useState<string | undefined>(initialQuery);
    const [selected, setSelected] = useState<string>();
    const [isDropdownVisible, setIsDropdownVisible] = useState(false);
    const appConfig = useAppConfig();
    const [isFocused, setIsFocused] = useState(false);
    const { quickFilters, selectedQuickFilter, setSelectedQuickFilter } = useQuickFiltersContext();
    const isShowSeparateSiblingsEnabled = useIsShowSeparateSiblingsEnabled();
    const userUrn = useUserContext().user?.urn;
    const finalCombineSiblings = isShowSeparateSiblingsEnabled ? false : combineSiblings;
    const searchViewAll = useSearchViewAll();
    const effectiveQuery = searchQuery !== undefined ? searchQuery : initialQuery || '';
    const showAutoCompleteResults = appConfig?.config?.featureFlags?.showAutoCompleteResults;

    useEffect(() => setSelected(initialQuery), [initialQuery]);

    const { data: recommendationData } = useListRecommendationsQuery({
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

    const quickFilterAutoCompleteOption = useMemo(() => {
        if (!showQuickFilters) {
            return null;
        }
        if (!showAutoCompleteResults) {
            // If we've disabled showing any autocomplete results, we also hide the "Filter By" flow.
            return null;
        }
        if (!quickFilters?.length) {
            return null;
        }
        return {
            label: <EntityTypeLabel>Filter by</EntityTypeLabel>,
            options: [
                {
                    value: 'quick-filter-unique-key',
                    type: '',
                    label: <QuickFilters searchQuery={searchQuery} setIsDropdownVisible={setIsDropdownVisible} />,
                    style: { padding: '8px', cursor: 'auto' },
                    disabled: true,
                },
            ],
        };
    }, [searchQuery, quickFilters, showAutoCompleteResults, showQuickFilters]);

    const emptyQueryOptions = useMemo(() => {
        const moduleOptions =
            recommendationData?.listRecommendations?.modules?.map((module) => ({
                label: <EntityTypeLabel>{module.title}</EntityTypeLabel>,
                options: [...module.content.map((content) => renderRecommendedQuery(content.value))],
            })) || [];

        return moduleOptions;
    }, [recommendationData?.listRecommendations?.modules]);

    const autoCompleteQueryOptions = useMemo(() => {
        if (effectiveQuery === '' || !showViewAllResults) return [];

        return [
            {
                value: `${EXACT_SEARCH_PREFIX}${effectiveQuery}`,
                label: <ViewAllSearchItem searchTarget={effectiveQuery} />,
                type: EXACT_AUTOCOMPLETE_OPTION_TYPE,
            },
        ];
    }, [effectiveQuery, showViewAllResults]);

    const autoCompleteEntityOptions = useMemo(() => {
        return suggestions.map((suggestion: AutoCompleteResultForEntity) => {
            const combinedSuggestion = combineSiblingsInAutoComplete(suggestion, {
                combineSiblings: finalCombineSiblings,
            });
            return {
                label: <SectionHeader entityType={combinedSuggestion.type} />,
                options: combinedSuggestion.combinedEntities.map((combinedEntity) => ({
                    value: combinedEntity.entity.urn,
                    label: (
                        <AutoCompleteItem
                            query={effectiveQuery}
                            entity={combinedEntity.entity}
                            siblings={finalCombineSiblings ? combinedEntity.matchedEntities : undefined}
                        />
                    ),
                    type: combinedEntity.entity.type,
                    style: { padding: '12px 12px 12px 16px' },
                })),
            };
        });
    }, [finalCombineSiblings, effectiveQuery, suggestions]);

    const previousSelectedQuickFilterValue = usePrevious(selectedQuickFilter?.value);

    useEffect(() => {
        // if we change the selected quick filter, re-issue auto-complete
        if (searchQuery && selectedQuickFilter?.value !== previousSelectedQuickFilterValue) {
            onQueryChange?.(searchQuery);
        }
    });

    // clear quick filters when this search bar is unmounted (ie. going from search results to home page)
    useEffect(() => {
        return () => {
            setSelectedQuickFilter(null);
        };
    }, [setSelectedQuickFilter]);

    const onClickExploreAll = useCallback(() => {
        searchViewAll();
        setIsDropdownVisible(false);
    }, [searchViewAll]);

    const options = useMemo(() => {
        const autoCompleteOptions =
            showAutoCompleteResults && autoCompleteEntityOptions.length ? autoCompleteEntityOptions : emptyQueryOptions;
        const quickFilterOptions = quickFilterAutoCompleteOption ? [quickFilterAutoCompleteOption] : [];
        const baseOptions: any[] = [...autoCompleteQueryOptions, ...quickFilterOptions, ...autoCompleteOptions];

        if (showViewAllResults) {
            baseOptions.push({
                value: 'explore-all-unique-key',
                type: '',
                label: (
                    <Button type="link" onClick={onClickExploreAll}>
                        Explore all →
                    </Button>
                ),
                style: { marginLeft: 'auto', cursor: 'auto' },
                disabled: true,
            });
        }
        return baseOptions;
    }, [
        emptyQueryOptions,
        autoCompleteEntityOptions,
        autoCompleteQueryOptions,
        quickFilterAutoCompleteOption,
        showViewAllResults,
        showAutoCompleteResults,
        onClickExploreAll,
    ]);

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
        setIsFocused(true);
    }

    function handleBlur() {
        if (onBlur) onBlur();
        handleSearchBarClick(false);
        setIsFocused(false);
    }

    function handleSearch(query: string, appliedQuickFilters?: FacetFilterInput[]) {
        onSearch(query, appliedQuickFilters);
        if (selectedQuickFilter) {
            setSelectedQuickFilter(null);
        }
    }

    const searchInputRef = useRef(null);

    useEffect(() => {
        if (showCommandK) {
            const handleKeyDown = (event) => {
                const isMac = (navigator as any).userAgentData
                    ? (navigator as any).userAgentData.platform.toLowerCase().includes('mac')
                    : navigator.userAgent.toLowerCase().includes('mac');

                // Support command-k to select the search bar on all platforms
                // Support ctrl-k to select the search bar on non-Mac platforms
                // 75 is the keyCode for 'k'
                if ((event.metaKey || (!isMac && event.ctrlKey)) && event.keyCode === 75) {
                    (searchInputRef?.current as any)?.focus();
                }
            };
            document.addEventListener('keydown', handleKeyDown);
            return () => {
                document.removeEventListener('keydown', handleKeyDown);
            };
        }
        return () => null;
    }, [showCommandK]);

    const viewsEnabledStyle = {
        ...style,
        backgroundColor: inputStyle?.backgroundColor,
    };

    return (
        <AutoCompleteContainer
            viewsEnabled={viewsEnabled}
            $isShowNavBarRedesign={isShowNavBarRedesign}
            id={id}
            style={viewsEnabled ? viewsEnabledStyle : style}
            ref={searchBarWrapperRef}
        >
            {isLoading ? (
                <SkeletonContainer>
                    <SkeletonButton shape="square" active block />
                </SkeletonContainer>
            ) : (
                <StyledAutoComplete
                    data-testid="search-bar"
                    $isShowNavBarRedesign={isShowNavBarRedesign}
                    defaultActiveFirstOption={false}
                    style={autoCompleteStyle}
                    options={options}
                    filterOption={false}
                    onSelect={(value, option) => {
                        // If the autocomplete option type is NOT an entity, then render as a normal search query.
                        if (
                            option.type === EXACT_AUTOCOMPLETE_OPTION_TYPE ||
                            option.type === RELEVANCE_QUERY_OPTION_TYPE
                        ) {
                            handleSearch(
                                `${filterSearchQuery(value as string)}`,
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
                    onSearch={showAutoCompleteResults ? onQueryChange : undefined}
                    defaultValue={initialQuery || undefined}
                    value={selected}
                    onChange={(v) => setSelected(filterSearchQuery(v as string))}
                    dropdownStyle={{
                        maxHeight: 1000,
                        overflowY: 'visible',
                        position: (fixAutoComplete && 'fixed') || 'relative',
                        ...(isShowNavBarRedesign ? { minWidth: '435px' } : {}),
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
                                getFiltersWithQuickFilter(selectedQuickFilter),
                            );
                        }}
                        style={{ ...inputStyle, color: '#fff' }}
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        data-testid="search-input"
                        onFocus={handleFocus}
                        onBlur={handleBlur}
                        viewsEnabled={viewsEnabled}
                        $isShowNavBarRedesign={isShowNavBarRedesign}
                        allowClear={(isFocused && { clearIcon: <ClearIcon /> }) || false}
                        prefix={
                            <>
                                <SearchIcon
                                    $isShowNavBarRedesign={isShowNavBarRedesign}
                                    onClick={() => {
                                        handleSearch(
                                            filterSearchQuery(searchQuery || ''),
                                            getFiltersWithQuickFilter(selectedQuickFilter),
                                        );
                                    }}
                                />
                            </>
                        }
                        ref={searchInputRef}
                        suffix={<>{(showCommandK && !isFocused && <CommandK />) || null}</>}
                        $textColor={textColor}
                        $placeholderColor={placeholderColor}
                    />
                </StyledAutoComplete>
            )}
            {viewsEnabled && (
                <ViewSelectContainer id={V2_SEARCH_BAR_VIEWS}>
                    <ViewSelect />
                </ViewSelectContainer>
            )}
        </AutoCompleteContainer>
    );
};

SearchBar.defaultProps = defaultProps;
