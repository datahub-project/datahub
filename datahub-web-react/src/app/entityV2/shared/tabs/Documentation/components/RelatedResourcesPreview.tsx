import React, { useMemo, useState } from 'react';
import styled from 'styled-components';

import ResourceItemPill from '@app/entityV2/shared/tabs/Documentation/components/ResourceItemPill';
import ResourcesModal from '@app/entityV2/shared/tabs/Documentation/components/ResourcesModal';
import {
    RelatedItem,
    getResourcePreview,
} from '@app/entityV2/shared/tabs/Documentation/components/relatedSectionUtils';
import { Button } from '@src/alchemy-components';

import { InstitutionalMemoryMetadata } from '@types';

const List = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-top: 8px;
`;

type Props = {
    items: RelatedItem[];
    canRemoveDocuments: boolean;
    showMoreLabel: (count: number) => string;
    onDocumentClick: (documentUrn: string) => void;
    onDocumentRemove: (documentUrn: string) => void;
    onLinkEdit: (link: InstitutionalMemoryMetadata) => void;
    onLinkDelete: (link: InstitutionalMemoryMetadata) => void;
};

export default function RelatedResourcesPreview({
    items,
    canRemoveDocuments,
    showMoreLabel,
    onDocumentClick,
    onDocumentRemove,
    onLinkEdit,
    onLinkDelete,
}: Props) {
    const [isModalOpen, setIsModalOpen] = useState(false);
    const { previewItems, remainingCount } = useMemo(() => getResourcePreview(items), [items]);

    const closeThen =
        <T,>(callback: (value: T) => void) =>
        (value: T) => {
            setIsModalOpen(false);
            callback(value);
        };

    return (
        <>
            {previewItems.length > 0 && (
                <List data-testid="related-list">
                    {previewItems.map((item) => (
                        <ResourceItemPill
                            key={item.type === 'link' ? `link-${item.data.url}` : `document-${item.data.urn}`}
                            item={item}
                            canRemoveDocuments={canRemoveDocuments}
                            onDocumentClick={onDocumentClick}
                            onDocumentRemove={onDocumentRemove}
                            onLinkEdit={onLinkEdit}
                            onLinkDelete={onLinkDelete}
                        />
                    ))}
                    {remainingCount > 0 && (
                        <Button
                            variant="text"
                            color="gray"
                            size="sm"
                            onClick={() => setIsModalOpen(true)}
                            data-testid="show-more-resources-button"
                        >
                            {showMoreLabel(remainingCount)}
                        </Button>
                    )}
                </List>
            )}
            {isModalOpen && (
                <ResourcesModal
                    items={items}
                    onClose={() => setIsModalOpen(false)}
                    onDocumentClick={closeThen(onDocumentClick)}
                    onDocumentRemove={closeThen(onDocumentRemove)}
                    canRemoveDocuments={canRemoveDocuments}
                    onLinkEdit={closeThen(onLinkEdit)}
                    onLinkDelete={closeThen(onLinkDelete)}
                />
            )}
        </>
    );
}
