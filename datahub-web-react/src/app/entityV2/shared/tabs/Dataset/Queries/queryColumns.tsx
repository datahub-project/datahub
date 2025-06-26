import DeleteOutlinedIcon from '@mui/icons-material/DeleteOutlined';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { Modal, Typography, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import ActorAvatar from '@app/entityV2/shared/ActorAvatar';
import { ActionButton } from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import QueryBuilderModal from '@app/entityV2/shared/tabs/Dataset/Queries/QueryBuilderModal';
import { Query } from '@app/entityV2/shared/tabs/Dataset/Queries/types';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import MarkdownViewer from '@src/app/entity/shared/components/legacy/MarkdownViewer';

import { useDeleteQueryMutation } from '@graphql/query.generated';
import { CorpUser, EntityType } from '@types';

/*
 * Description Column
 */

const StyledLink = styled(Typography.Link)`
    display: block;
`;

const TruncatedTextWrapper = styled.div`
    display: inline;
`;

const MAX_DESCRIPTION_LENGTH = 50;

interface DescriptionProps {
    description?: string;
}

export const QueryDescription = ({ description }: DescriptionProps) => {
    const [isTruncated, setIsTruncated] = useState(description && description.length > MAX_DESCRIPTION_LENGTH);

    if (!description) return null;

    const truncatedDescription = description.slice(0, MAX_DESCRIPTION_LENGTH);

    return (
        <div>
            {isTruncated && (
                <>
                    <TruncatedTextWrapper>
                        <MarkdownViewer source={`${truncatedDescription}...`} />
                    </TruncatedTextWrapper>
                    <StyledLink onClick={() => setIsTruncated(false)}>Read more</StyledLink>
                </>
            )}
            {!isTruncated && (
                <>
                    <MarkdownViewer source={description} ignoreLimit />
                    {description.length > MAX_DESCRIPTION_LENGTH && (
                        <StyledLink onClick={() => setIsTruncated(true)}>Read less</StyledLink>
                    )}
                </>
            )}
        </div>
    );
};

/*
 * Created By Column
 */

const INGESTION_URN = 'urn:li:corpuser:_ingestion';

interface CreatedByProps {
    createdBy?: CorpUser;
}

export const QueryCreatedBy = ({ createdBy }: CreatedByProps) => {
    const entityRegistry = useEntityRegistryV2();

    if (!createdBy || createdBy.urn === INGESTION_URN) return null;

    const userName = entityRegistry.getDisplayName(EntityType.CorpUser, createdBy);

    return (
        <div>
            <ActorAvatar
                size={26}
                name={userName}
                url={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${createdBy.urn}`}
                photoUrl={
                    createdBy?.editableProperties?.pictureLink || createdBy?.editableInfo?.pictureLink || undefined
                }
            />
        </div>
    );
};

/*
 * Edit/Delete Column
 */

const ButtonsWrapper = styled.span<{ $isHidden: boolean }>`
    display: flex;
    gap: 8px;
    align-items: center;
    justify-content: center;
    ${(props) => props.$isHidden && `visibility: hidden;`}
`;

interface EditDeleteProps {
    query: Query;
    hoveredQueryUrn: string | null;
    onEdited?: (query) => void;
    onDeleted?: (query) => void;
}

export const EditDeleteColumn = ({ query, hoveredQueryUrn, onEdited, onDeleted }: EditDeleteProps) => {
    const [editingQuery, setEditingQuery] = useState<Query | null>(null);
    const [deleteQueryMutation] = useDeleteQueryMutation();
    const urn = query.urn as string;

    const deleteQuery = () => {
        deleteQueryMutation({ variables: { urn } })
            .then(({ errors }) => {
                if (!errors) {
                    message.success({
                        content: `Deleted Query!`,
                        duration: 3,
                    });
                    onDeleted?.(query);
                }
            })
            .catch(() => {
                message.destroy();
                message.error({ content: 'Failed to delete Query! An unexpected error occurred' });
            });
    };

    const confirmDeleteQuery = () => {
        Modal.confirm({
            title: `Delete Query`,
            content: `Are you sure you want to delete this query?`,
            onOk() {
                deleteQuery();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const onEditSubmitted = (newQuery) => {
        setEditingQuery(null);
        onEdited?.(newQuery);
    };

    return (
        <>
            <ButtonsWrapper $isHidden={hoveredQueryUrn !== query.urn}>
                <ActionButton privilege onClick={() => setEditingQuery(query)} data-testid="edit-query">
                    <EditOutlinedIcon />
                </ActionButton>
                <ActionButton privilege onClick={confirmDeleteQuery} data-testid="delete-query">
                    <DeleteOutlinedIcon />
                </ActionButton>
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
        </>
    );
};

interface ColumnProps {
    query: Query;
}

const ColumnsWrapper = styled.div`
    text-align: right;
`;

/*
 * Columns Column
 */
export const ColumnsColumn = ({ query }: ColumnProps) => {
    return <ColumnsWrapper>{query.columns?.length ?? 0}</ColumnsWrapper>;
};
