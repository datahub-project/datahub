import { Checkbox, Divider, List, ListProps } from 'antd';
import React, { useCallback, useEffect, useMemo, useState } from 'react';
import styled from 'styled-components';
import { SearchResult, SearchSuggestion } from '../../types.generated';
import analytics, { EventType } from '../analytics';
import { ANTD_GRAY } from '../entity/shared/constants';
import { EntityAndType } from '../entity/shared/types';
import { SEARCH_COLORS } from '../entityV2/shared/constants';
import { PreviewSection } from '../shared/MatchesContext';
import { useEntityRegistry } from '../useEntityRegistry';
import EmptySearchResults from './EmptySearchResults';
import { MatchContextContainer } from './matches/MatchContextContainer';
import { useIsSearchV2 } from './useSearchAndBrowseVersion';
import { CombinedSearchResult } from './utils/combineSiblingsInSearchResults';
import { PreviewType } from '../entity/Entity';
import { useInitializeSearchResultCards } from '../entityV2/shared/components/styled/search/useInitializeSearchResultCards';
import { useSearchContext } from '../search/context/SearchContext';
import { useShowNavBarRedesign } from '../useShowNavBarRedesign';

export const MATCHES_CONTAINER_HEIGHT = 52;

const ResultList = styled(List)<{ $isShowNavBarRedesign?: boolean }>`
    &&& {
        margin-top: 8px;
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        padding: ${(props) => (props.$isShowNavBarRedesign ? '0px 3px 0px 12px' : '0px 12px 0px 16px')};
        border-radius: 0px;
    }
`;

const StyledCheckbox = styled(Checkbox)`
    margin-right: 12px;
`;

const ThinDivider = styled(Divider)`
    margin-top: 16px;
    margin-bottom: 16px;
`;

export const ResultWrapper = styled.div<{
    showUpdatedStyles: boolean;
    selected: boolean;
    areMatchesExpanded: boolean;
    isFullViewCard: boolean;
    $isShowNavBarRedesign?: boolean;
}>`
    // z-index: 2;
    ${(props) =>
        props.showUpdatedStyles &&
        `    
        background-color: white;
        border-radius: ${props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
        padding: 16px 20px;
        box-shadow: 0px 0px 5px rgba(0, 0, 0, 0.08);
        border-bottom: 1px solid ${ANTD_GRAY[5]};
        cursor: pointer;
        :hover {
            ${!props.selected && `outline: 1px solid ${SEARCH_COLORS.TITLE_PURPLE};}`};
        }
    `}
    position: relative;
    overflow: hidden;
    ${(props) =>
        props.selected &&
        `
        outline: 1px solid ${SEARCH_COLORS.TITLE_PURPLE};
        border-left: 5px solid ${SEARCH_COLORS.TITLE_PURPLE};
        left: -5px;
        width: calc(100% + 5px);
        position: relative;
    `}
    margin-bottom: ${(props) => {
        let marginBottomValue;
        if (props.areMatchesExpanded) {
            marginBottomValue = MATCHES_CONTAINER_HEIGHT + 20;
        } else if (props.isFullViewCard) {
            marginBottomValue = props.$isShowNavBarRedesign ? 16 : 20;
        } else {
            marginBottomValue = 10;
        }
        return marginBottomValue;
    }}px;
    transition: margin-bottom 0.3s ease;
    ${(props) =>
        props.areMatchesExpanded &&
        `
        -webkit-box-shadow: 0px 0px 24px 0px rgba(0, 0, 0, 0.15);
        -moz-box-shadow: 0px 0px 24px 0px rgba(0, 0, 0, 0.15);
         box-shadow: 0px 0px 24px 0px rgba(0, 0, 0, 0.15);
    `}
`;

const ListItem = styled.div<{ isSelectMode: boolean }>`
    display: flex;
    align-items: center;
`;

function useSearchKeyboardControls(
    highlightedIndex: number | null,
    setHighlightedIndex: (i: number | null) => void,
    setHighlightedByKeyboardIndex: (v: [number | null]) => void,
    searchResults: CombinedSearchResult[],
) {
    return useCallback(
        (event: KeyboardEvent) => {
            const prevIndex = highlightedIndex;
            let newIndex: number | null | undefined;
            if (event.key === 'ArrowDown') {
                event.preventDefault();
                newIndex = prevIndex === null ? null : Math.min(prevIndex + 1, searchResults.length - 1);
                setHighlightedByKeyboardIndex([newIndex]);
            } else if (event.key === 'ArrowUp') {
                event.preventDefault();
                newIndex = prevIndex === null ? null : Math.max(prevIndex - 1, 0);
                setHighlightedByKeyboardIndex([newIndex]);
            } else if (event.key === 'Escape') {
                newIndex = null;
            }
            if (newIndex !== undefined) {
                setHighlightedIndex(newIndex);
            }
        },
        [highlightedIndex, setHighlightedIndex, setHighlightedByKeyboardIndex, searchResults.length],
    );
}

type Props = {
    loading: boolean;
    query: string;
    searchResults: CombinedSearchResult[];
    totalResultCount: number;
    isSelectMode: boolean;
    selectedEntities: EntityAndType[];
    setSelectedEntities: (entities: EntityAndType[]) => any;
    suggestions: SearchSuggestion[];
    highlightedIndex: number | null;
    setHighlightedIndex: (val: number | null) => void;
    pageNumber: number;
    previewType?: PreviewType;
    onCardClick?: (any: any) => any;
    setAreAllEntitiesSelected?: (areAllSelected: boolean) => void;
};

export const SearchResultList = ({
    loading,
    query,
    searchResults,
    totalResultCount,
    isSelectMode,
    selectedEntities,
    setSelectedEntities,
    suggestions,
    highlightedIndex,
    setHighlightedIndex,
    pageNumber,
    previewType,
    onCardClick,
    setAreAllEntitiesSelected,
}: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const entityRegistry = useEntityRegistry();
    const selectedEntityUrns = selectedEntities.map((entity) => entity.urn);
    const showSearchFiltersV2 = useIsSearchV2();
    const refs = useMemo(
        () => Array.from({ length: searchResults.length }).map(() => React.createRef<HTMLDivElement>()),
        [searchResults.length],
    );
    // Used to scroll on arrow key up / down, but not on mouse scrolling
    // Stores a pointer to a number, rather than number directly, so we scroll even if you hit up at the top of the page
    const [highlightedByKeyboardIndex, setHighlightedByKeyboardIndex] = useState<[number | null]>([highlightedIndex]);

    const onKeyPress = useSearchKeyboardControls(
        highlightedIndex,
        setHighlightedIndex,
        setHighlightedByKeyboardIndex,
        searchResults,
    );

    useEffect(() => {
        document.addEventListener('keydown', onKeyPress);
        return () => {
            document.removeEventListener('keydown', onKeyPress);
        };
    }, [onKeyPress]);

    useEffect(() => {
        if (highlightedByKeyboardIndex[0] !== null) {
            refs[highlightedByKeyboardIndex[0]]?.current?.scrollIntoView({
                behavior: 'smooth',
                block: 'center',
            });
        }
    }, [highlightedByKeyboardIndex, refs]);

    const onClickResult = (result: SearchResult, index: number) => {
        analytics.event({
            type: EventType.SearchResultClickEvent,
            query,
            entityUrn: result.entity.urn,
            entityType: result.entity.type,
            index,
            total: totalResultCount,
            pageNumber,
        });
        setHighlightedIndex(index);
    };

    /**
     * Invoked when a new entity is selected. Simply updates the state of the list of selected entities.
     */
    const onSelectEntity = (selectedEntity: EntityAndType, selected: boolean) => {
        if (selected) {
            setSelectedEntities?.([...selectedEntities, selectedEntity]);
        } else {
            setSelectedEntities?.(selectedEntities?.filter((entity) => entity.urn !== selectedEntity.urn) || []);
            setAreAllEntitiesSelected?.(false);
        }
    };

    const [urnToExpandedSection, setUrnToExpandedSection] = React.useState<Record<string, PreviewSection>>({});
    // default open the matches section if there are matches
    useInitializeSearchResultCards(searchResults, setUrnToExpandedSection);
    const { isFullViewCard } = useSearchContext();

    return (
        <ResultList<React.FC<ListProps<CombinedSearchResult>>>
            id="search-result-list"
            $isShowNavBarRedesign={isShowNavBarRedesign}
            dataSource={searchResults}
            split={false}
            locale={{ emptyText: (!loading && <EmptySearchResults suggestions={suggestions} />) || <></> }}
            renderItem={(item, index) => {
                const expandedSection = isFullViewCard ? urnToExpandedSection[item.entity.urn] : undefined;
                return (
                    <div style={{ position: 'relative' }}>
                        <MatchContextContainer
                            item={item}
                            selected={highlightedIndex === index}
                            onClick={() => onClickResult(item, index)}
                            urnToExpandedSection={urnToExpandedSection}
                            setUrnToExpandedSection={setUrnToExpandedSection}
                        >
                            <ResultWrapper
                                showUpdatedStyles={showSearchFiltersV2}
                                className={`entityUrn-${item.entity.urn}`}
                                selected={highlightedIndex === index}
                                areMatchesExpanded={!!expandedSection}
                                onClick={() => onClickResult(item, index)}
                                ref={refs[index]}
                                isFullViewCard={isFullViewCard}
                                $isShowNavBarRedesign={isShowNavBarRedesign}
                            >
                                <ListItem
                                    isSelectMode={isSelectMode}
                                    // class name for counting in test purposes only
                                    className="test-search-result"
                                >
                                    {isSelectMode && (
                                        <StyledCheckbox
                                            checked={selectedEntityUrns.indexOf(item.entity.urn) >= 0}
                                            onChange={(e) =>
                                                onSelectEntity(
                                                    {
                                                        urn: item.entity.urn,
                                                        type: item.entity.type,
                                                    },
                                                    e.target.checked,
                                                )
                                            }
                                            onClick={(e) => e.stopPropagation()}
                                        />
                                    )}
                                    {entityRegistry.renderSearchResult(
                                        item.entity.type,
                                        item,
                                        previewType,
                                        onCardClick,
                                    )}
                                </ListItem>
                                {/* an entity is always going to be inserted in the sibling group, so if the sibling group is just one do not
                        render. */}
                                {!showSearchFiltersV2 && <ThinDivider />}
                            </ResultWrapper>
                        </MatchContextContainer>
                    </div>
                );
            }}
        />
    );
};
