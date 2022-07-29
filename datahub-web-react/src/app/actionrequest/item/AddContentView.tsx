import { Typography } from 'antd';
import React from 'react';
import { ActionRequest } from '../../../types.generated';
import CreatedByView from './CreatedByView';
import RequestTargetEntityView from './RequestTargetEntityView';

interface Props {
    showActionsButtons: boolean;
    requestMetadataView: React.ReactNode;
    actionRequest: ActionRequest;
}

function AddContentView({ showActionsButtons, requestMetadataView, actionRequest }: Props) {
    return (
        <span>
            <CreatedByView actionRequest={actionRequest} />
            <Typography.Text> requests to add </Typography.Text>
            {requestMetadataView}
            {showActionsButtons && <Typography.Text>{` to `}</Typography.Text>}
            {showActionsButtons && <RequestTargetEntityView actionRequest={actionRequest} />}
        </span>
    );
}

export default AddContentView;
