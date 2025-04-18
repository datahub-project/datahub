import React from 'react';
import { ActionRequest } from '../../../../types.generated';
import MetadataAssociationRequestItem from '../MetadataAssociationRequestItem';
import UpdateDescriptionContentView from './UpdateDescriptionContentView';

type Props = {
    actionRequest: ActionRequest;
    onUpdate: () => void;
    showActionsButtons: boolean;
};

const REQUEST_TYPE_DISPLAY_NAME = 'Update Description Proposal';

export default function UpdateDescriptionRequestItem({ actionRequest, onUpdate, showActionsButtons }: Props) {
    const contentView = <UpdateDescriptionContentView actionRequest={actionRequest} />;

    return (
        <MetadataAssociationRequestItem
            requestTypeDisplayName={REQUEST_TYPE_DISPLAY_NAME}
            requestContentView={contentView}
            actionRequest={actionRequest}
            onUpdate={onUpdate}
            showActionsButtons={showActionsButtons}
        />
    );
}
