import { useCallback, useState } from 'react';

import { useLinkUtils } from '@app/entityV2/shared/components/links/useLinkUtils';

import { InstitutionalMemoryMetadata } from '@types';

/**
 * Wires up the edit + delete affordances for a list of institutional-memory links.
 *
 * Consumers render a `ResourceLinkPill` per link with `onEdit`/`onDelete` pointed at
 * this hook, then render an `EditLinkModal` gated on `isEditModalOpen`. Deleting a link
 * removes it immediately (no confirmation) via `useLinkUtils`, matching the summary
 * About section and Documentation tab. Extracted so those surfaces don't each re-declare
 * the same modal state.
 */
export function useLinkListActions() {
    const [editingMetadata, setEditingMetadata] = useState<InstitutionalMemoryMetadata>();
    const [isEditModalOpen, setIsEditModalOpen] = useState(false);
    const { handleDeleteLink } = useLinkUtils(editingMetadata);

    const onEdit = useCallback((metadata: InstitutionalMemoryMetadata) => {
        setEditingMetadata(metadata);
        setIsEditModalOpen(true);
    }, []);

    const onCloseEditModal = useCallback(() => {
        setEditingMetadata(undefined);
        setIsEditModalOpen(false);
    }, []);

    return { editingMetadata, isEditModalOpen, onEdit, onCloseEditModal, handleDeleteLink };
}
