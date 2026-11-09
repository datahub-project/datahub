import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { FileMagnifyingGlass } from '@phosphor-icons/react/dist/csr/FileMagnifyingGlass';
import { Layout } from '@phosphor-icons/react/dist/csr/Layout';
import { Tag } from '@phosphor-icons/react/dist/csr/Tag';
import { UserCircle } from '@phosphor-icons/react/dist/csr/UserCircle';
import React, { useContext } from 'react';
import styled from 'styled-components';

import { EntityCapabilityType } from '@app/entityV2/Entity';
import { LineageTabContext } from '@app/entityV2/shared/tabs/Lineage/LineageTabContext';
import SearchPill, {
    PILL_DOWNSTREAM_COLUMN_COUNT,
    PILL_MATCH_COUNT,
    PILL_OWNER_COUNT,
    PILL_TAG_COUNT,
    PILL_TERM_COUNT,
    PILL_UPSTREAM_COLUMN_COUNT,
} from '@app/previewV2/SearchPill';
import { entityHasCapability, getHighlightedTag } from '@app/previewV2/utils';
import { useMatchedFieldsForList } from '@app/search/context/SearchResultContext';
import MatchesContext, { PreviewSection } from '@app/sharedV2/MatchesContext';
import { isRemovedAssociation } from '@app/sharedV2/tags/tags.utils';

import { EntityPath, EntityType, GlobalTags, GlossaryTerms, LineageDirection, Owner } from '@types';

const PillsContainer = styled.div`
    gap: 5px;
    display: flex;
    flex-direction: row;
    align-items: center;
    height: 30px;
`;

interface Props {
    glossaryTerms?: GlossaryTerms;
    tags?: GlobalTags;
    owners?: Array<Owner> | null;
    entityCapabilities: Set<EntityCapabilityType>;
    paths?: EntityPath[];
    entityType: EntityType;
}

const Pills = ({ glossaryTerms, tags, owners, entityCapabilities, paths, entityType }: Props) => {
    const { lineageDirection, isColumnLevelLineage, selectedColumn } = useContext(LineageTabContext);
    const { setExpandedSection, expandedSection } = useContext(MatchesContext);
    const groupedMatches = useMatchedFieldsForList('fieldLabels');
    const showGlossaryTermsBadge = entityHasCapability(entityCapabilities, EntityCapabilityType.GLOSSARY_TERMS);
    const showTagsBadge = entityHasCapability(entityCapabilities, EntityCapabilityType.TAGS);
    const showOwnersBadge = entityHasCapability(entityCapabilities, EntityCapabilityType.OWNERS);
    const highlightedTag = getHighlightedTag(tags);
    const liveTerms = glossaryTerms?.terms?.filter((association) => !isRemovedAssociation(association)) ?? [];
    const liveTags = tags?.tags?.filter((association) => !isRemovedAssociation(association)) ?? [];

    const handlePillClick = (section: PreviewSection | undefined, data) => (e) => {
        if (!data?.length) return;
        setExpandedSection(expandedSection === section ? undefined : section);
        e.preventDefault();
        e.stopPropagation();
    };

    return (
        <PillsContainer>
            {showGlossaryTermsBadge && glossaryTerms && (
                <SearchPill
                    icon={<BookmarkSimple />}
                    count={liveTerms.length}
                    enabled={!!liveTerms.length}
                    active={expandedSection === PreviewSection.GLOSSARY_TERMS}
                    label=""
                    countLabelKey={PILL_TERM_COUNT}
                    onClick={handlePillClick(PreviewSection.GLOSSARY_TERMS, liveTerms)}
                    highlightedText={liveTerms[0]?.term?.properties?.name ?? ''}
                />
            )}
            {showTagsBadge && tags && (
                <SearchPill
                    icon={<Tag />}
                    count={liveTags.length}
                    enabled={!!liveTags.length}
                    active={expandedSection === PreviewSection.TAGS}
                    label=""
                    countLabelKey={PILL_TAG_COUNT}
                    onClick={handlePillClick(PreviewSection.TAGS, liveTags)}
                    highlightedText={highlightedTag}
                />
            )}
            {showOwnersBadge && owners && (
                <SearchPill
                    icon={<UserCircle />}
                    count={owners.length || 0}
                    enabled={!!owners.length}
                    active={expandedSection === PreviewSection.OWNERS}
                    label=""
                    countLabelKey={PILL_OWNER_COUNT}
                    onClick={handlePillClick(PreviewSection.OWNERS, owners)}
                />
            )}

            {groupedMatches.length > 0 && (
                <SearchPill
                    icon={<FileMagnifyingGlass />}
                    count={groupedMatches?.length || 0}
                    enabled
                    active={expandedSection === PreviewSection.MATCHES}
                    label=""
                    countLabelKey={PILL_MATCH_COUNT}
                    onClick={handlePillClick(PreviewSection.MATCHES, groupedMatches)}
                />
            )}
            {/* only show the column paths pill on datasets who actually have columns to show */}
            {paths &&
                paths.length > 0 &&
                entityType === EntityType.Dataset &&
                isColumnLevelLineage &&
                selectedColumn && (
                    <SearchPill
                        data-testid="show-column-path-button"
                        icon={<Layout />}
                        count={paths.length || 0}
                        enabled={!!paths.length}
                        active={expandedSection === PreviewSection.COLUMN_PATHS}
                        label=""
                        countLabelKey={
                            lineageDirection === LineageDirection.Downstream
                                ? PILL_DOWNSTREAM_COLUMN_COUNT
                                : PILL_UPSTREAM_COLUMN_COUNT
                        }
                        onClick={handlePillClick(PreviewSection.COLUMN_PATHS, paths)}
                    />
                )}
        </PillsContainer>
    );
};

export default Pills;
