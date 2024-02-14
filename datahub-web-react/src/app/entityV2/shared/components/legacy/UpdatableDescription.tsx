import { message, Tag } from 'antd';
import React, { useState } from 'react';
import { FetchResult, MutationFunctionOptions } from '@apollo/client';
import styled from 'styled-components';
import MarkdownViewer from './MarkdownViewer';
import UpdateDescriptionModal from './DescriptionModal';
import analytics, { EventType, EntityActionType } from '../../../../analytics';
import { EntityType } from '../../../../../types.generated';

const DescriptionText = styled(MarkdownViewer)`
    ${(props) => (props.isCompact ? 'max-width: 377px;' : '')};
`;

const AddNewDescription = styled(Tag)`
    cursor: pointer;
`;

export type Props = {
    isCompact?: boolean;
    updateEntity: (options?: MutationFunctionOptions<any, any> | undefined) => Promise<FetchResult>;
    updatedDescription?: string | null;
    originalDescription?: string | null;
    entityType: EntityType;
    urn: string;
};

export default function UpdatableDescription({
    isCompact,
    updateEntity,
    updatedDescription,
    originalDescription,
    entityType,
    urn,
}: Props) {
    const [showAddDescModal, setShowAddDescModal] = useState(false);
    const onSubmit = async (description: string | null) => {
        message.loading({ content: 'Updating...' });
        try {
            await updateEntity({
                variables: { urn, input: { editableProperties: { description: description || '' } } },
            });
            message.destroy();
            analytics.event({
                type: EventType.EntityActionEvent,
                actionType: EntityActionType.UpdateDescription,
                entityType,
                entityUrn: urn,
            });
            message.success({ content: 'Updated!', duration: 2 });
        } catch (e: unknown) {
            message.destroy();
            if (e instanceof Error) message.error({ content: `Update Failed! \n ${e.message || ''}`, duration: 2 });
        }
        setShowAddDescModal(false);
    };

    return (
        <>
            {updatedDescription || originalDescription ? (
                <>
                    <DescriptionText
                        isCompact={isCompact}
                        source={updatedDescription || originalDescription || ''}
                        editable
                        onEditClicked={() => setShowAddDescModal(true)}
                    />
                    {showAddDescModal && (
                        <UpdateDescriptionModal
                            title="Update description"
                            onClose={() => setShowAddDescModal(false)}
                            onSubmit={onSubmit}
                            original={originalDescription || ''}
                            description={updatedDescription || ''}
                        />
                    )}
                </>
            ) : (
                <>
                    <AddNewDescription color="success" onClick={() => setShowAddDescModal(true)}>
                        + Add Description
                    </AddNewDescription>
                    {showAddDescModal && (
                        <UpdateDescriptionModal
                            title="Add description"
                            onClose={() => setShowAddDescModal(false)}
                            onSubmit={onSubmit}
                            isAddDesc
                        />
                    )}
                </>
            )}
        </>
    );
}
