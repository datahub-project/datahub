import { Avatar, Button, toast } from '@components';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { AvatarType } from '@components/components/AvatarStack/types';

import QueryBuilderModal from '@app/entityV2/shared/tabs/Dataset/Queries/QueryBuilderModal';
import { Query } from '@app/entityV2/shared/tabs/Dataset/Queries/types';
import CompactMarkdownViewer from '@app/entityV2/shared/tabs/Documentation/components/CompactMarkdownViewer';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { ActorWithDisplayNameFragment, useDeleteQueryMutation } from '@graphql/query.generated';

/*
 * Description Column
 */

interface DescriptionProps {
    description?: string;
}

export const QueryDescription = ({ description }: DescriptionProps) => {
    if (!description) return null;

    // CompactMarkdownViewer renders through the sanitizing alchemy Editor (DOMPurify) and
    // provides its own Show more/Show less truncation, so raw HTML in a query description
    // cannot execute (guards against stored XSS).
    return <CompactMarkdownViewer content={description} />;
};

/*
 * Created By Column
 */

const INGESTION_URN = 'urn:li:corpuser:_ingestion';

interface CreatedByProps {
    createdBy?: ActorWithDisplayNameFragment;
}

export const QueryCreatedBy = ({ createdBy }: CreatedByProps) => {
    const entityRegistry = useEntityRegistryV2();

    if (!createdBy || createdBy.urn === INGESTION_URN) return null;

    const userName = entityRegistry.getDisplayName(createdBy.type, createdBy);
    const photoUrl = createdBy?.editableProperties?.pictureLink || createdBy?.editableInfo?.pictureLink || undefined;

    return <Avatar name={userName || ''} imageUrl={photoUrl} type={AvatarType.user} size="sm" showInPill />;
};

/*
 * Edit/Delete Column
 */

const ButtonsWrapper = styled.span`
    display: flex;
    gap: 8px;
    align-items: center;
    justify-content: center;
`;

interface EditDeleteProps {
    query: Query;
    onEdited?: (query) => void;
    onDeleted?: (query) => void;
}

export const EditDeleteColumn = ({ query, onEdited, onDeleted }: EditDeleteProps) => {
    const { t } = useTranslation('entity.profile.queries');
    const { t: tc } = useTranslation('common.actions');
    const [editingQuery, setEditingQuery] = useState<Query | null>(null);
    const [showConfirmationModal, setShowConfirmationModal] = useState(false);
    const [deleteQueryMutation] = useDeleteQueryMutation();
    const urn = query.urn as string;

    const deleteQuery = () => {
        setShowConfirmationModal(false);
        deleteQueryMutation({ variables: { urn } })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(t('queryCard.deleteSuccess'), { duration: 3 });
                    onDeleted?.(query);
                }
            })
            .catch(() => {
                toast.destroy();
                toast.error(t('queryCard.deleteError'));
            });
    };

    const onEditSubmitted = (newQuery) => {
        setEditingQuery(null);
        onEdited?.(newQuery);
    };

    return (
        <>
            <ButtonsWrapper>
                <Button
                    variant="text"
                    color="gray"
                    size="sm"
                    isCircle
                    icon={{ icon: PencilSimple }}
                    onClick={() => setEditingQuery(query)}
                    data-testid="edit-query"
                    aria-label={tc('edit')}
                />
                <Button
                    variant="text"
                    color="red"
                    size="sm"
                    isCircle
                    icon={{ icon: Trash }}
                    onClick={() => setShowConfirmationModal(true)}
                    data-testid="delete-query"
                    aria-label={tc('delete')}
                />
            </ButtonsWrapper>
            {editingQuery && (
                <QueryBuilderModal
                    initialState={{
                        urn: editingQuery.urn as string,
                        title: editingQuery.title,
                        description: editingQuery.description,
                        query: editingQuery.query,
                    }}
                    onSubmit={onEditSubmitted}
                    onClose={() => setEditingQuery(null)}
                />
            )}
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={() => setShowConfirmationModal(false)}
                handleConfirm={deleteQuery}
                modalTitle={t('queryCard.deleteConfirmTitle')}
                modalText={t('queryCard.deleteConfirmBody')}
                confirmButtonText={tc('delete')}
                isDeleteModal
            />
        </>
    );
};

interface ColumnProps {
    query: Query;
}

/*
 * Columns Column
 */
export const ColumnsColumn = ({ query }: ColumnProps) => {
    return <div>{query.columns?.length ?? 0}</div>;
};
