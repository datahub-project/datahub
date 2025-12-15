import { Button } from '@components';
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { AddRelatedEntityDropdown } from '@app/entityV2/document/summary/AddRelatedEntityDropdown';
import { EntityLink } from '@app/homeV2/reference/sections/EntityLink';
import { useEntityRegistry } from '@app/useEntityRegistry';
import colors from '@src/alchemy-components/theme/foundations/colors';

import { AndFilterInput, DocumentRelatedAsset, DocumentRelatedDocument, EntityType, FilterOperator } from '@types';

const Section = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;

    &:hover {
        .hover-btn {
            display: flex;
        }
    }
    padding-top: 20px;
`;

const SectionHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const SectionTitle = styled.h4`
    font-size: 14px;
    font-weight: 600;
    margin: 0;
    color: ${colors.gray[600]};
`;

const List = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const EmptyState = styled.div`
    font-size: 14px;
    font-weight: 400;
    color: ${colors.gray[1800]};
    text-align: start;
    padding: 0px;
`;

const EntityItemContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    width: 100%;

    /* Show trash button only on hover */
    .trash-button {
        opacity: 0;
        transition: opacity 0.2s ease;
    }

    &:hover .trash-button {
        opacity: 1;
    }
`;

const EntityLinkWrapper = styled.div`
    flex: 1;
    min-width: 0; /* Allow flex item to shrink below content size */
`;

const TrashButton = styled(Button)`
    flex-shrink: 0;
    padding: 0;
`;

interface RelatedSectionProps {
    relatedAssets?: DocumentRelatedAsset[];
    relatedDocuments?: DocumentRelatedDocument[];
    documentUrn: string;
    onAddEntities: (assetUrns: string[], documentUrns: string[]) => Promise<void>;
    onRemoveEntity?: (urn: string) => Promise<void>;
    canEdit?: boolean;
}

export const RelatedSection: React.FC<RelatedSectionProps> = ({
    relatedAssets,
    relatedDocuments,
    documentUrn,
    onAddEntities,
    onRemoveEntity,
    canEdit = true,
}) => {
    const entityRegistry = useEntityRegistry();
    const userContext = useUserContext();

    // Entity types that can be related (based on RelatedAsset.pdl)
    // I don't love this, but I do want to enable searching for documents
    // as well, even though not supported in the primary search bar yet.
    const allowedEntityTypes: EntityType[] = [
        EntityType.Container,
        EntityType.Dataset,
        EntityType.DataJob,
        EntityType.DataFlow,
        EntityType.Dashboard,
        EntityType.Chart,
        EntityType.Application,
        EntityType.DataPlatform,
        EntityType.Mlmodel,
        EntityType.MlmodelGroup,
        EntityType.MlprimaryKey,
        EntityType.MlfeatureTable,
        EntityType.CorpUser,
        EntityType.CorpGroup,
        EntityType.DataProduct,
        EntityType.Domain,
        EntityType.GlossaryTerm,
        EntityType.GlossaryNode,
        EntityType.Tag,
        EntityType.Document, // Also allow documents
    ];

    // Combine all related entities
    const allRelatedEntities = [
        ...(relatedAssets?.map((ra) => ({ ...ra.asset, isDocument: false })) || []),
        ...(relatedDocuments?.map((rd) => ({ ...rd.document, isDocument: true })) || []),
    ];

    // Get all existing URNs to prevent duplicates (excluding the document itself)
    const existingUrns = useMemo(
        () =>
            new Set([
                ...(relatedAssets?.map((ra) => ra.asset.urn) || []),
                ...(relatedDocuments?.map((rd) => rd.document.urn) || []),
            ]),
        [relatedAssets, relatedDocuments],
    );

    // Get view URN from user context
    const viewUrn = userContext?.localState?.selectedViewUrn || undefined;

    // Create default filters to exclude unpublished documents
    // Filter: state = PUBLISHED
    // Note: This filter applies to documents. For other entity types that don't have a "state" field,
    // the backend should handle this gracefully (either ignore the filter or the field check fails gracefully).
    // If this causes issues with non-documents being excluded, we may need to restructure the filter.
    const defaultFilters: AndFilterInput[] = useMemo(
        () => [
            {
                and: [
                    {
                        field: 'state',
                        condition: FilterOperator.Equal,
                        negated: true,
                        values: ['UNPUBLISHED'],
                    },
                ],
            },
        ],
        [],
    );

    const handleConfirmAdd = async (selectedUrns: string[]) => {
        // Separate documents from assets
        // selectedUrns is the final list (after user selections/deselections)
        const finalDocumentUrns: string[] = [];
        const finalAssetUrns: string[] = [];

        selectedUrns.forEach((urn) => {
            if (urn.includes(':document:')) {
                finalDocumentUrns.push(urn);
            } else {
                finalAssetUrns.push(urn);
            }
        });

        // Pass the final lists (this will replace the entire list, handling both additions and removals)
        await onAddEntities(finalAssetUrns, finalDocumentUrns);
    };

    return (
        <Section>
            <SectionHeader>
                <SectionTitle>Related</SectionTitle>
                {canEdit && (
                    <AddRelatedEntityDropdown
                        entityTypes={allowedEntityTypes}
                        existingUrns={existingUrns}
                        documentUrn={documentUrn}
                        onConfirm={handleConfirmAdd}
                        placeholder="Find related assets and context..."
                        defaultFilters={defaultFilters}
                        viewUrn={viewUrn}
                        initialSelectedUrns={Array.from(existingUrns)}
                    />
                )}
            </SectionHeader>
            <List>
                {allRelatedEntities.length > 0 ? (
                    allRelatedEntities.map((entity) => {
                        const genericProperties = entityRegistry.getGenericEntityProperties(
                            entity.type as EntityType,
                            entity,
                        );

                        return (
                            <EntityItemContainer key={entity.urn}>
                                <EntityLinkWrapper>
                                    <EntityLink
                                        entity={genericProperties}
                                        showHealthIcon={!entity.isDocument}
                                        showDeprecatedIcon={!entity.isDocument}
                                    />
                                </EntityLinkWrapper>
                                {canEdit && onRemoveEntity && (
                                    <TrashButton
                                        variant="text"
                                        icon={{ icon: 'Trash', source: 'phosphor', color: 'red' }}
                                        size="md"
                                        className="trash-button"
                                        onClick={(e) => {
                                            e.preventDefault();
                                            e.stopPropagation();
                                            onRemoveEntity(entity.urn);
                                        }}
                                        aria-label="Remove related entity"
                                        data-testid={`remove-related-entity-${entity.urn.split(':').pop()}`}
                                    />
                                )}
                            </EntityItemContainer>
                        );
                    })
                ) : (
                    <EmptyState>Add related assets or context</EmptyState>
                )}
            </List>
        </Section>
    );
};
