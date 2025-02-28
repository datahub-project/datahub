import React from 'react';
import styled from 'styled-components';
import { SEARCH_COLORS } from '../../entityV2/shared/constants';
import MatchContext, { PreviewSection } from '../../shared/MatchesContext';
import { SearchCardSlideoutContent } from '../searchSlideout/SearchCardSlideoutContent';
import { CombinedSearchResult } from '../utils/combineSiblingsInSearchResults';
import HorizontalScroller from '../../sharedV2/carousel/HorizontalScroller';
import { useSearchContext } from '../../search/context/SearchContext';

const MATCHES_CONTAINER_HEIGHT = 52;

const MatchesContainer = styled.div<{ expanded: boolean; selected: boolean; compactUserSearchCardStyle: boolean }>`
    z-index: 1;
    border-radius: 10px;
    margin: 0 auto 12px auto;
    padding: 4px;
    :hover {
        ${(props) => !props.selected && `outline: 1px solid ${SEARCH_COLORS.TITLE_PURPLE};}`};
    }

    position: absolute;
    height: ${(props) =>
        !props.expanded || props.compactUserSearchCardStyle ? '100%' : `calc(100% + ${MATCHES_CONTAINER_HEIGHT}px)`};
    transition: height 0.3s ease;

    // height: 100%;
    width: 100%;
    background-color: #ffffff;
    display: flex;
    flex-direction: column;

    ${(props) =>
        props.selected &&
        `
        outline: 1px solid ${SEARCH_COLORS.TITLE_PURPLE};
        left: -5px;
        width: calc(100% + 5px);
    `}
`;

const MatchesBottomGroup = styled.div`
    margin-top: auto;
    margin-bottom: 8px;
    /* The following div ensures that Tags, Terms, etc., override their default flex-wrap: wrap; with flex-wrap: nowrap; 
    and maintain alignment as needed, making them scrollable horizontally */
    div {
        display: flex;
        flex-wrap: nowrap;
        align-items: center;
        height: 100%;
    }
`;

type Props = {
    item: CombinedSearchResult;
    selected: boolean;
    onClick: (e: React.MouseEvent) => void;
    children: React.ReactNode;
    urnToExpandedSection: Record<string, PreviewSection>;
    setUrnToExpandedSection: (urnToExpandedSection: Record<string, PreviewSection>) => void;
    compactUserSearchCardStyle?: boolean;
};

/**
 * This component is a wrapper around the search results that provides the context for
 * rendering the matches. It also handles the logic for expanding and collapsing the
 * search result match drawer.
 *
 * You can wrap it around any search result in a search list to enable the match drawer.
 * The state is managed in the parent component in order to enable expand all and collapse all in the future.
 */
export const MatchContextContainer = ({
    item,
    selected,
    onClick,
    children,
    urnToExpandedSection,
    setUrnToExpandedSection,
    compactUserSearchCardStyle,
}: Props) => {
    const { isFullViewCard } = useSearchContext();
    const expandedSection = isFullViewCard ? urnToExpandedSection[item.entity.urn] : undefined;

    return (
        <MatchContext.Provider
            value={{
                expandedSection,
                setExpandedSection: (section?: PreviewSection) => {
                    if (section) {
                        setUrnToExpandedSection({
                            ...urnToExpandedSection,
                            [item.entity.urn]: section,
                        });
                    } else {
                        // eslint-disable-next-line @typescript-eslint/no-unused-vars
                        const { [item.entity.urn]: removedElement, ...newUrnToExpandedSection } = urnToExpandedSection;
                        setUrnToExpandedSection(newUrnToExpandedSection);
                    }
                },
            }}
        >
            {isFullViewCard && (
                <MatchesContainer
                    expanded={!!expandedSection}
                    selected={selected}
                    onClick={onClick}
                    compactUserSearchCardStyle={compactUserSearchCardStyle || false}
                >
                    <MatchesBottomGroup>
                        <MatchContext.Provider
                            value={{
                                expandedSection: undefined,
                                setExpandedSection: () => {},
                            }}
                        >
                            <HorizontalScroller alwaysVisible scrollDistance={300}>
                                <SearchCardSlideoutContent item={item} expandedSection={expandedSection} />
                            </HorizontalScroller>
                        </MatchContext.Provider>
                    </MatchesBottomGroup>
                </MatchesContainer>
            )}
            {/* {children} */}
            <div style={{ position: 'relative', zIndex: 6 }}>{children}</div>
        </MatchContext.Provider>
    );
};
