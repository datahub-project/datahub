import { FileText } from '@phosphor-icons/react';
import React from 'react';

import { GenericEntityProperties } from '@app/entity/shared/types';
import { PreviewType } from '@app/entityV2/Entity';
import { EntityMenuItems } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import DefaultPreviewCard from '@app/previewV2/DefaultPreviewCard';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Document, DocumentState, EntityType, Owner, SearchInsight } from '@types';

export const Preview = ({
    document,
    urn,
    data,
    name,
    description,
    platformName,
    platformLogo,
    platformInstanceId,
    owners,
    insights,
    logoComponent,
    headerDropdownItems,
    previewType,
}: {
    document: Document;
    urn: string;
    data: GenericEntityProperties | null;
    name: string;
    description?: string | null;
    platformName?: string;
    platformLogo?: string | null;
    platformInstanceId?: string;
    owners?: Array<Owner> | null;
    insights?: Array<SearchInsight> | null;
    logoComponent?: JSX.Element;
    headerDropdownItems?: Set<EntityMenuItems>;
    previewType: PreviewType;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const status = document.info?.status?.state;
    const isPublished = status === DocumentState.Published;

    // Get external URL from source if document is external
    const externalUrl = document.info?.source?.externalUrl || null;

    // Truncate description for preview
    const truncatedDescription =
        description && description.length > 200 ? `${description.substring(0, 200)}...` : description;

    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.Document, urn)}
            name={name || ''}
            urn={urn}
            data={data}
            description={truncatedDescription || ''}
            entityType={EntityType.Document}
            platform={platformName}
            logoUrl={platformLogo || undefined}
            platformInstanceId={platformInstanceId}
            typeIcon={<FileText size={14} color="#BFBFBF" weight="duotone" />}
            owners={owners}
            insights={insights}
            logoComponent={logoComponent}
            parentEntities={
                document.parentDocuments?.documents ? (document.parentDocuments.documents as any[]) : undefined
            }
            entityIcon={<FileText size={28} color={isPublished ? '#1890ff' : '#8c8c8c'} weight="duotone" />}
            externalUrl={externalUrl}
            headerDropdownItems={headerDropdownItems}
            previewType={previewType}
        />
    );
};
