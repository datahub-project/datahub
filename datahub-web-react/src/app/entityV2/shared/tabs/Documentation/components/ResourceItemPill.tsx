import React from 'react';

import { ResourceDocumentPill } from '@app/entityV2/shared/tabs/Documentation/components/ResourceDocumentPill';
import { ResourceLinkPill } from '@app/entityV2/shared/tabs/Documentation/components/ResourceLinkPill';
import { RelatedItem } from '@app/entityV2/shared/tabs/Documentation/components/relatedSectionUtils';

import { InstitutionalMemoryMetadata } from '@types';

type Props = {
    item: RelatedItem;
    canRemoveDocuments: boolean;
    onDocumentClick: (documentUrn: string) => void;
    onDocumentRemove: (documentUrn: string) => void;
    onLinkEdit: (link: InstitutionalMemoryMetadata) => void;
    onLinkDelete: (link: InstitutionalMemoryMetadata) => void;
};

export default function ResourceItemPill({
    item,
    canRemoveDocuments,
    onDocumentClick,
    onDocumentRemove,
    onLinkEdit,
    onLinkDelete,
}: Props) {
    if (item.type === 'link') {
        return <ResourceLinkPill link={item.data} onEdit={onLinkEdit} onDelete={onLinkDelete} />;
    }

    return (
        <ResourceDocumentPill
            document={item.data}
            onClick={onDocumentClick}
            onRemove={onDocumentRemove}
            canRemove={canRemoveDocuments}
        />
    );
}
