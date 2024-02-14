import React, { useEffect, useState } from 'react';

import styled from 'styled-components';
import { ExpandedOwner } from '../../entity/shared/components/styled/ExpandedOwner/ExpandedOwner';
import { PreviewSection } from '../../shared/MatchesContext';
import TagTermGroup from '../../sharedV2/tags/TagTermGroup';
import { useEntityRegistry } from '../../useEntityRegistry';
import { CombinedSearchResult } from '../utils/combineSiblingsInSearchResults';

type Props = {
    item?: CombinedSearchResult | null;
    expandedSection?: PreviewSection;
};

const OwnerContainer = styled.div`
    margin-bottom: -6px;
`;

// slide out preview content for search cards
export const SearchCardSlideoutContent = ({ item, expandedSection }: Props) => {
    // we cache the expanded section so that we can keep the expanded section open when the user collapses the slideout
    const [cachedExpandedSection, setCachedExpandedSection] = useState<PreviewSection | undefined>(expandedSection);
    useEffect(() => {
        if (!expandedSection) return;
        setCachedExpandedSection(expandedSection);
    }, [expandedSection]);

    const entityRegistry = useEntityRegistry();

    if (!item || !item.entity || !cachedExpandedSection) return <></>;

    const genericProps = entityRegistry.getGenericEntityProperties(item?.entity?.type, item?.entity);

    if (cachedExpandedSection === PreviewSection.MATCHES) {
        return entityRegistry.renderSearchMatches(item.entity.type, item);
    }

    if (cachedExpandedSection === PreviewSection.TAGS) {
        return (
            <TagTermGroup
                editableTags={genericProps?.globalTags}
                showEmptyMessage
                entityUrn={genericProps?.urn || ''}
                entityType={genericProps?.type}
                readOnly
                fontSize={12}
            />
        );
    }

    if (cachedExpandedSection === PreviewSection.GLOSSARY_TERMS) {
        return (
            <TagTermGroup
                editableGlossaryTerms={genericProps?.glossaryTerms}
                showEmptyMessage
                entityUrn={genericProps?.urn || ''}
                entityType={genericProps?.type}
                readOnly
                fontSize={12}
            />
        );
    }

    if (cachedExpandedSection === PreviewSection.OWNERS) {
        return (
            <OwnerContainer>
                {genericProps?.ownership?.owners?.map((owner) => (
                    <ExpandedOwner key={owner.owner.urn} entityUrn={genericProps?.urn || ''} owner={owner} readOnly />
                ))}
            </OwnerContainer>
        );
    }

    return <>TODO: {cachedExpandedSection}</>;
};
