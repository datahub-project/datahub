import { LayoutOutlined } from '@ant-design/icons';
import AccountCircleOutlinedIcon from '@mui/icons-material/AccountCircleOutlined';
import FindInPageOutlinedIcon from '@mui/icons-material/FindInPageOutlined';
import SellOutlinedIcon from '@mui/icons-material/SellOutlined';
import { BookmarkSimple } from '@phosphor-icons/react';
import React, { useContext } from 'react';
import styled from 'styled-components';

import { EntityCapabilityType } from '@app/entityV2/Entity';
import { LineageTabContext } from '@app/entityV2/shared/tabs/Lineage/LineageTabContext';
import SearchPill from '@app/previewV2/SearchPill';
import { entityHasCapability, getHighlightedTag } from '@app/previewV2/utils';
import { useMatchedFieldsForList } from '@app/search/context/SearchResultContext';
import MatchesContext, { PreviewSection } from '@app/shared/MatchesContext';

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
    const lineageDirectionText = lineageDirection === LineageDirection.Downstream ? 'downstream' : 'upstream';
    const { setExpandedSection, expandedSection } = useContext(MatchesContext);
    const groupedMatches = useMatchedFieldsForList('fieldLabels');
    const showGlossaryTermsBadge = entityHasCapability(entityCapabilities, EntityCapabilityType.GLOSSARY_TERMS);
    const showTagsBadge = entityHasCapability(entityCapabilities, EntityCapabilityType.TAGS);
    const showOwnersBadge = entityHasCapability(entityCapabilities, EntityCapabilityType.OWNERS);
    const highlightedTag = getHighlightedTag(tags);

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
                    count={glossaryTerms.terms?.length || 0}
                    enabled={!!glossaryTerms.terms?.length}
                    active={expandedSection === PreviewSection.GLOSSARY_TERMS}
                    label=""
                    countLabel="term"
                    onClick={handlePillClick(PreviewSection.GLOSSARY_TERMS, glossaryTerms.terms)}
                    highlightedText={glossaryTerms.terms?.length ? glossaryTerms?.terms[0]?.term?.properties?.name : ''}
                />
            )}
            {showTagsBadge && tags && (
                <SearchPill
                    icon={<SellOutlinedIcon />}
                    count={tags.tags?.length || 0}
                    enabled={!!tags.tags?.length}
                    active={expandedSection === PreviewSection.TAGS}
                    label=""
                    countLabel="tag"
                    onClick={handlePillClick(PreviewSection.TAGS, tags.tags)}
                    highlightedText={highlightedTag}
                />
            )}
            {showOwnersBadge && owners && (
                <SearchPill
                    icon={<AccountCircleOutlinedIcon />}
                    count={owners.length || 0}
                    enabled={!!owners.length}
                    active={expandedSection === PreviewSection.OWNERS}
                    label=""
                    countLabel="owner"
                    onClick={handlePillClick(PreviewSection.OWNERS, owners)}
                />
            )}

            {groupedMatches.length > 0 && (
                <SearchPill
                    icon={<FindInPageOutlinedIcon />}
                    count={groupedMatches?.length || 0}
                    enabled
                    active={expandedSection === PreviewSection.MATCHES}
                    label=""
                    countLabel="match"
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
                        icon={<LayoutOutlined />}
                        count={paths.length || 0}
                        enabled={!!paths.length}
                        active={expandedSection === PreviewSection.COLUMN_PATHS}
                        label=""
                        countLabel={`${lineageDirectionText} column`}
                        onClick={handlePillClick(PreviewSection.COLUMN_PATHS, paths)}
                    />
                )}
        </PillsContainer>
    );
};

export default Pills;
