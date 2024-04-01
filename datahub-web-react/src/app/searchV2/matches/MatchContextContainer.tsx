import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { SEARCH_COLORS } from '../../entityV2/shared/constants';
import MatchContext, { PreviewSection } from '../../shared/MatchesContext';
import { SearchCardSlideoutContent } from '../searchSlideout/SearchCardSlideoutContent';
import { CombinedSearchResult } from '../utils/combineSiblingsInSearchResults';

const MATCHES_CONTAINER_HEIGHT = 52;

const MatchesContainer = styled.div<{ expanded: boolean; selected: boolean }>`
    z-index: 1;
    border-radius: 8px;
    margin: 0 auto 12px auto;
    padding: 8px 19px;
    box-shadow: 0px 0px 5px rgba(0, 0, 0, 0.08);
    border-bottom: 1px solid ${ANTD_GRAY[5]};
    :hover {
        ${(props) => !props.selected && `outline: 1px solid ${SEARCH_COLORS.TITLE_PURPLE};}`};
    }

    position: absolute;
    height: ${(props) => (!props.expanded ? '100%' : `calc(100% + ${MATCHES_CONTAINER_HEIGHT}px)`)};
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
        // border-left: 5px solid ${SEARCH_COLORS.TITLE_PURPLE};
        left: -5px;
        width: calc(100% + 5px);
    `}
`;

const MatchesBottomGroup = styled.div`
    margin-top: auto;
`;

type Props = {
    item: CombinedSearchResult;
    selected: boolean;
    onClick: (e: React.MouseEvent) => void;
    children: React.ReactNode;
    urnToExpandedSection: Record<string, PreviewSection>;
    setUrnToExpandedSection: (urnToExpandedSection: Record<string, PreviewSection>) => void;
};

/**
 * This component is a wrapper around the search results that provides the context for
 * rendering the matches. It also handles the logic for expanding and collapsing the
 * search result match drawer.
 *
 * You can wrap it around any search result in a search list to enable the match drawer.
 * The state is managed in the parent component in order to enable expand all and collapse all in the future.
 */
export const MatchContextContianer = ({
    item,
    selected,
    onClick,
    children,
    urnToExpandedSection,
    setUrnToExpandedSection,
}: Props) => {
    const expandedSection = urnToExpandedSection[item.entity.urn];

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
            <MatchesContainer expanded={!!expandedSection} selected={selected} onClick={onClick}>
                <MatchesBottomGroup>
                    <MatchContext.Provider
                        value={{
                            expandedSection: undefined,
                            setExpandedSection: () => {},
                        }}
                    >
                        <SearchCardSlideoutContent item={item} expandedSection={expandedSection} />
                    </MatchContext.Provider>
                </MatchesBottomGroup>
            </MatchesContainer>
            {/* {children} */}
            <div style={{ position: 'relative', zIndex: 6 }}>{children}</div>
        </MatchContext.Provider>
    );
};
