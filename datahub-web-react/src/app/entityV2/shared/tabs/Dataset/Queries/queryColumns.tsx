import { Modal, Popover, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import DeleteOutlinedIcon from '@mui/icons-material/DeleteOutlined';
import CompactMarkdownViewer from '../../Documentation/components/CompactMarkdownViewer';
import { CorpUser, EntityType } from '../../../../../../types.generated';
import { useEntityRegistryV2 } from '../../../../../useEntityRegistry';
import ActorAvatar from '../../../ActorAvatar';
import { Query } from './types';
import { ActionButton } from '../../../containers/profile/sidebar/SectionActionButton';
import QueryBuilderModal from './QueryBuilderModal';
import { useDeleteQueryMutation } from '../../../../../../graphql/query.generated';
import { PopularityBars } from '../Schema/components/SchemaFieldDrawer/PopularityBars';
import {
    getBarsStatusFromPopularityTier,
    getQueryPopularityTier,
} from '../../../containers/profile/sidebar/shared/utils';

interface DescriptionProps {
    description?: string;
}

export const QueryDescription = ({ description }: DescriptionProps) => {
    if (!description) return null;

    return <CompactMarkdownViewer content={description} fixedLineHeight />;
};

/*
 * Created By Column
 */

const UserNameWrapper = styled.span`
    margin-left: 4px;
`;

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
            <UserNameWrapper>{userName}</UserNameWrapper>
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
                <ActionButton privilege onClick={() => setEditingQuery(query)}>
                    <EditOutlinedIcon />
                </ActionButton>
                <ActionButton privilege onClick={confirmDeleteQuery}>
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
    if (!runsPercentileLast30days) return null;
    const tier = getQueryPopularityTier(runsPercentileLast30days);
    const status = getBarsStatusFromPopularityTier(tier);
    return (
        <Popover
            content={
                <>This query has been run more than {runsPercentileLast30days}% of other queries in the last 30 days.</>
            }
        >
            <PopularityWrapper>
                <PopularityBars status={status} />
            </PopularityWrapper>
        </Popover>
    );
};
