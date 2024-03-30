import React, { useEffect, useState } from 'react';

import styled from 'styled-components';
import { ExpandedOwner } from '../../entity/shared/components/styled/ExpandedOwner/ExpandedOwner';
import { PreviewSection } from '../../shared/MatchesContext';
import TagTermGroup from '../../sharedV2/tags/TagTermGroup';
import { useEntityRegistryV2 } from '../../useEntityRegistry';
import { CombinedSearchResult } from '../utils/combineSiblingsInSearchResults';
import EntityPaths from '../../previewV2/EntityPaths/EntityPaths';

type Props = {
    item?: CombinedSearchResult | null;
    expandedSection?: PreviewSection;
};

const PaddingContainer = styled.div`
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

    const entityRegistry = useEntityRegistryV2();

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
            <PaddingContainer>
                {genericProps?.ownership?.owners?.map((owner) => (
                    <ExpandedOwner key={owner.owner.urn} entityUrn={genericProps?.urn || ''} owner={owner} readOnly />
                ))}
            </PaddingContainer>
        );
    }

    if (cachedExpandedSection === PreviewSection.COLUMN_PATHS) {
        return (
            <PaddingContainer>
                <EntityPaths paths={item.paths || []} resultEntityUrn={item.entity.urn} />
            </PaddingContainer>
        );
    }

    return <>TODO: {cachedExpandedSection}</>;
};
