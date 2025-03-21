import { LayoutOutlined } from '@ant-design/icons';
import { MatchesGroupedByFieldName } from '@app/searchV2/matches/constants';
import React, { useContext } from 'react';
import AccountCircleOutlinedIcon from '@mui/icons-material/AccountCircleOutlined';
import FindInPageOutlinedIcon from '@mui/icons-material/FindInPageOutlined';
import SellOutlinedIcon from '@mui/icons-material/SellOutlined';
import styled from 'styled-components';
import { BookmarkSimple } from '@phosphor-icons/react';
import { EntityPath, GlossaryTermAssociation, LineageDirection, Owner, TagAssociation } from '../../types.generated';
import MatchesContext, { PreviewSection } from '../shared/MatchesContext';
import SearchPill from './SearchPill';
import { getHighlightedTag } from './utils';

const PillsContainer = styled.div`
    gap: 5px;
    display: flex;
    flex-direction: row;
    align-items: center;
    height: 30px;
`;

interface Props {
    glossaryTerms: GlossaryTermAssociation[];
    tags: TagAssociation[];
    owners: Owner[];
    groupedMatches: MatchesGroupedByFieldName[];
    paths?: EntityPath[];
    lineageDirection: LineageDirection;
}

const Pills = ({ glossaryTerms, tags, owners, groupedMatches, paths, lineageDirection }: Props) => {
    const lineageDirectionText = lineageDirection === LineageDirection.Downstream ? 'downstream' : 'upstream';
    const { setExpandedSection, expandedSection } = useContext(MatchesContext);
    const highlightedTag = getHighlightedTag(tags);

    const handlePillClick = (section: PreviewSection | undefined, data) => (e) => {
        if (!data?.length) return;
        setExpandedSection(expandedSection === section ? undefined : section);
        e.preventDefault();
        e.stopPropagation();
    };

    return (
        <PillsContainer>
            {!!glossaryTerms.length && (
                <SearchPill
                    icon={<BookmarkSimple />}
                    count={glossaryTerms.length || 0}
                    enabled={!!glossaryTerms.length}
                    active={expandedSection === PreviewSection.GLOSSARY_TERMS}
                    label=""
                    countLabel="term"
                    onClick={handlePillClick(PreviewSection.GLOSSARY_TERMS, glossaryTerms)}
                    highlightedText={glossaryTerms.length ? glossaryTerms[0]?.term?.properties?.name : ''}
                />
            )}
            {!!tags.length && (
                <SearchPill
                    icon={<SellOutlinedIcon />}
                    count={tags.length}
                    enabled={!!tags.length}
                    active={expandedSection === PreviewSection.TAGS}
                    label=""
                    countLabel="tag"
                    onClick={handlePillClick(PreviewSection.TAGS, tags)}
                    highlightedText={highlightedTag}
                />
            )}
            {!!owners.length && (
                <SearchPill
                    icon={<AccountCircleOutlinedIcon />}
                    count={owners.length}
                    enabled={!!owners.length}
                    active={expandedSection === PreviewSection.OWNERS}
                    label=""
                    countLabel="owner"
                    onClick={handlePillClick(PreviewSection.OWNERS, owners)}
                />
            )}

            {!!groupedMatches.length && (
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
            {!!paths?.length && (
                <SearchPill
                    icon={<LayoutOutlined />}
                    count={paths.length}
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
