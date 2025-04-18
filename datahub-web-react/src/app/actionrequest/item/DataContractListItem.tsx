import React from 'react';
import { ActionRequest } from '../../../types.generated';
import CreateDataContractContentView from './CreateDataContractContentView';
import MetadataAssociationRequestItem from './MetadataAssociationRequestItem';

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
