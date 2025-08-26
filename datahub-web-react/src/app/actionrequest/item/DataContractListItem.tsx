import React from 'react';

import CreateDataContractContentView from '@app/actionrequest/item/CreateDataContractContentView';
import MetadataAssociationRequestItem from '@app/actionrequest/item/MetadataAssociationRequestItem';

import { ActionRequest } from '@types';

type Props = {
    actionRequest: ActionRequest;
    onUpdate: () => void;
    showActionsButtons: boolean;
};

const REQUEST_TYPE_DISPLAY_NAME = 'Data Contract Proposal';

export default function DataContractListItem({ actionRequest, onUpdate, showActionsButtons }: Props) {
    const contentView = <CreateDataContractContentView actionRequest={actionRequest} />;

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
