import React, { useEffect, useState } from 'react';
import styled from 'styled-components';

import { ExpandedOwner } from '@app/entity/shared/components/styled/ExpandedOwner/ExpandedOwner';
import EntityPaths from '@app/previewV2/EntityPaths/EntityPaths';
import { useSearchContext } from '@app/search/context/SearchContext';
import { CombinedSearchResult } from '@app/searchV2/utils/combineSiblingsInSearchResults';
import { PreviewSection } from '@app/shared/MatchesContext';
import TagTermGroup from '@app/sharedV2/tags/TagTermGroup';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

type Props = {
    item?: CombinedSearchResult | null;
    expandedSection?: PreviewSection;
};

const PaddingContainer = styled.div`
    padding: 0px 8px;
`;

const OwnersContainer = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
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
    const { isFullViewCard } = useSearchContext();

    if (!item || !item.entity || !cachedExpandedSection || !isFullViewCard) return <></>;

    const genericProps = entityRegistry.getGenericEntityProperties(item?.entity?.type, item?.entity);

    let content: JSX.Element;
    switch (cachedExpandedSection) {
        case PreviewSection.MATCHES:
            content = entityRegistry.renderSearchMatches(item.entity.type, item);
            break;
        case PreviewSection.TAGS:
            content = (
                <TagTermGroup
                    editableTags={genericProps?.globalTags}
                    showEmptyMessage
                    entityUrn={genericProps?.urn || ''}
                    entityType={genericProps?.type}
                    readOnly
                    fontSize={12}
                />
            );
            break;
        case PreviewSection.GLOSSARY_TERMS:
            content = (
                <TagTermGroup
                    editableGlossaryTerms={genericProps?.glossaryTerms}
                    showEmptyMessage
                    entityUrn={genericProps?.urn || ''}
                    entityType={genericProps?.type}
                    readOnly
                    fontSize={12}
                />
            );
            break;
        case PreviewSection.OWNERS:
            content = (
                <OwnersContainer>
                    {genericProps?.ownership?.owners?.map((owner) => (
                        <ExpandedOwner
                            key={owner.owner.urn}
                            entityUrn={genericProps?.urn || ''}
                            owner={owner}
                            readOnly
                        />
                    ))}
                </OwnersContainer>
            );
            break;
        case PreviewSection.COLUMN_PATHS:
            content = <EntityPaths paths={item.paths || []} resultEntityUrn={item.entity.urn} />;
            break;
        default:
            content = <></>;
            break;
    }
    return <PaddingContainer>{content}</PaddingContainer>;
};
