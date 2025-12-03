import { Popover } from '@components';
import DeleteOutlinedIcon from '@mui/icons-material/DeleteOutlined';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import ActorAvatar from '@app/entityV2/shared/ActorAvatar';
import { ActionButton } from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import {
    getBarsStatusFromPopularityTier,
    getQueryPopularityTier,
} from '@app/entityV2/shared/containers/profile/sidebar/shared/utils';
import QueryBuilderModal from '@app/entityV2/shared/tabs/Dataset/Queries/QueryBuilderModal';
import { Query } from '@app/entityV2/shared/tabs/Dataset/Queries/types';
import { PopularityBars } from '@app/entityV2/shared/tabs/Dataset/Schema/components/SchemaFieldDrawer/PopularityBars';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import MarkdownViewer from '@src/app/entity/shared/components/legacy/MarkdownViewer';

import { ActorWithDisplayNameFragment, useDeleteQueryMutation } from '@graphql/query.generated';

/*
 * Description Column
 */

const TruncatedTextWrapper = styled.div`
    display: inline;
`;

const QueryDescriptionWrapper = styled.div`
    max-height: 300px;
    overflow-y: auto;
    overflow-x: hidden;
`;

interface DescriptionProps {
    description?: string;
}

export const QueryDescription = ({ description }: DescriptionProps) => {
    return (
        <QueryDescriptionWrapper>
            <TruncatedTextWrapper>
                <MarkdownViewer source={description || ''} limit={60} ignoreLimit={false} maxWidth={170} />
            </TruncatedTextWrapper>
        </QueryDescriptionWrapper>
    );
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

    return (
        <div>
            <ActorAvatar
                size={26}
                name={userName}
                url={`/${entityRegistry.getPathName(createdBy.type)}/${createdBy.urn}`}
                photoUrl={photoUrl}
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
    const [showConfirmationModal, setShowConfirmationModal] = useState(false);
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
                <ActionButton privilege onClick={() => setShowConfirmationModal(true)} data-testid="delete-query">
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
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={() => setShowConfirmationModal(false)}
                handleConfirm={deleteQuery}
                modalTitle="Delete Query"
                modalText="Are you sure you want to delete this query?"
            />
        </>
    );
};

/*
 * Popularity Column
 */

const PopularityWrapper = styled.div`
    display: flex;
    justify-content: center;
`;

interface PopularityColumnProps {
    query: Query;
}

export const PopularityColumn = ({ query }: PopularityColumnProps) => {
    const { runsPercentileLast30days } = query;
    if (!runsPercentileLast30days) return <div>-</div>;
    const tier = getQueryPopularityTier(runsPercentileLast30days);
    const status = getBarsStatusFromPopularityTier(tier);
    return (
        <Popover
            content={
                <>This query has been run more than {runsPercentileLast30days}% of other queries in the last 30 days.</>
            }
        >
            <PopularityWrapper data-testid="query-popularity">
                <PopularityBars status={status} />
            </PopularityWrapper>
        </Popover>
    );
};

/*
 * Columns Column
 */
export const ColumnsColumn = ({ query }: PopularityColumnProps) => {
    return <div>{query.columns?.length ?? 0}</div>;
};
