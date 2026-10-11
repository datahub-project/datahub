import { Pill, Text } from '@components';
import { X } from '@phosphor-icons/react/dist/csr/X';
import React, { useCallback, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { ALLOWED_RELATED_ASSET_TYPES, categorizeUrns } from '@app/document/utils/documentUtils';
import { IconStyleType } from '@app/entityV2/Entity';
import { AddRelatedEntityDropdown } from '@app/entityV2/document/summary/AddRelatedEntityDropdown';
import { useEntityRegistry } from '@app/useEntityRegistry';

import {
    AndFilterInput,
    DocumentRelatedAsset,
    DocumentRelatedDocument,
    Entity,
    EntityType,
    FilterOperator,
} from '@types';

// Document is appended separately since it's handled via relatedDocuments and is not
// in ALLOWED_RELATED_ASSET_TYPES.
const ALLOWED_ENTITY_TYPES: EntityType[] = [...Object.values(ALLOWED_RELATED_ASSET_TYPES), EntityType.Document];

// Default filter to exclude unpublished documents. For entity types without a "state"
// field, the backend should ignore or gracefully skip this filter.
const DEFAULT_FILTERS: AndFilterInput[] = [
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
];

const Divider = styled.div`
    border-top: 1px solid ${(props) => props.theme.colors.border};
`;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const SectionHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: center;
`;

const PillList = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
`;

type EntityPillIconProps = {
    platformLogoUrl?: string | null;
    entityType: EntityType;
    entityRegistry: ReturnType<typeof useEntityRegistry>;
};

// Renders a platform logo when available, falling back to the entity-type icon
// (either because there is no logo URL or because loading the logo failed).
function EntityPillIcon({ platformLogoUrl, entityType, entityRegistry }: EntityPillIconProps) {
    const [imgError, setImgError] = useState(false);
    if (platformLogoUrl && !imgError) {
        return (
            <img
                src={platformLogoUrl}
                alt=""
                style={{ height: 12, width: 'auto', objectFit: 'contain' }}
                onError={() => setImgError(true)}
            />
        );
    }
    return <>{entityRegistry.getIcon(entityType, 12, IconStyleType.ACCENT)}</>;
}

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
    const { t } = useTranslation('entity.types');
    const entityRegistry = useEntityRegistry();
    const userContext = useUserContext();
    const history = useHistory();

    const allRelatedEntities: Entity[] = useMemo(
        () => [
            ...(relatedAssets?.map((ra) => ra.asset as Entity) || []),
            ...(relatedDocuments?.map((rd) => rd.document as Entity) || []),
        ],
        [relatedAssets, relatedDocuments],
    );

    const existingUrns = useMemo(
        () =>
            new Set([
                ...(relatedAssets?.map((ra) => ra.asset.urn) || []),
                ...(relatedDocuments?.map((rd) => rd.document.urn) || []),
            ]),
        [relatedAssets, relatedDocuments],
    );

    const viewUrn = userContext?.localState?.selectedViewUrn || undefined;

    const handleConfirmAdd = useCallback(
        async (selectedUrns: string[]) => {
            const { assetUrns, documentUrns } = categorizeUrns(selectedUrns);
            await onAddEntities(assetUrns, documentUrns);
        },
        [onAddEntities],
    );

    return (
        <>
            <Divider />
            <Section>
                <SectionHeader>
                    <Text size="sm" weight="bold" color="text">
                        {t('document.relatedTitle')}
                    </Text>
                    {canEdit && (
                        <AddRelatedEntityDropdown
                            entityTypes={ALLOWED_ENTITY_TYPES}
                            existingUrns={existingUrns}
                            documentUrn={documentUrn}
                            onConfirm={handleConfirmAdd}
                            placeholder={t('document.findRelatedPlaceholder')}
                            defaultFilters={DEFAULT_FILTERS}
                            viewUrn={viewUrn}
                            initialSelectedUrns={Array.from(existingUrns)}
                        />
                    )}
                </SectionHeader>
                <PillList>
                    {allRelatedEntities.length > 0 ? (
                        allRelatedEntities.map((entity) => {
                            const entityType = entity.type as EntityType;
                            const genericProps = entityRegistry.getGenericEntityProperties(entityType, entity);
                            const platformLogoUrl = genericProps?.platform?.properties?.logoUrl;
                            const displayName = entityRegistry.getDisplayName(entityType, entity);
                            const url = entityRegistry.getEntityUrl(entityType, entity.urn);

                            return (
                                <Pill
                                    key={entity.urn}
                                    label={displayName}
                                    variant="filled"
                                    color="gray"
                                    size="md"
                                    clickable
                                    onPillClick={() => history.push(url)}
                                    customIconRenderer={() => (
                                        <EntityPillIcon
                                            platformLogoUrl={platformLogoUrl}
                                            entityType={entityType}
                                            entityRegistry={entityRegistry}
                                        />
                                    )}
                                    rightIcon={canEdit && onRemoveEntity ? X : undefined}
                                    onClickRightIcon={
                                        canEdit && onRemoveEntity
                                            ? (e) => {
                                                  e.stopPropagation();
                                                  onRemoveEntity(entity.urn);
                                              }
                                            : undefined
                                    }
                                />
                            );
                        })
                    ) : (
                        <Text size="sm" color="textTertiary">
                            {t('document.addRelatedEmpty')}
                        </Text>
                    )}
                </PillList>
            </Section>
        </>
    );
};
