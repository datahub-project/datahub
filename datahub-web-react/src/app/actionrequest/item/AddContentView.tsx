import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { ActionRequest, ActionRequestOrigin } from '../../../types.generated';
import CreatedByView from './CreatedByView';
import RequestTargetEntityView from './RequestTargetEntityView';
import AiActorLabel from './AiActorLabel';

const ContentWrapper = styled.span`
    font-size: 14px;
`;

interface Props {
    requestMetadataView: React.ReactNode;
    actionRequest: ActionRequest;
}

function AddContentView({ requestMetadataView, actionRequest }: Props) {
    const { origin } = actionRequest;
    return (
        <ContentWrapper>
            {origin === ActionRequestOrigin.Inferred ? (
                <AiActorLabel />
            ) : (
                <CreatedByView actionRequest={actionRequest} />
            )}
            <Typography.Text> requests to add </Typography.Text>
            {requestMetadataView}
            <Typography.Text>{` to `}</Typography.Text>
            <RequestTargetEntityView actionRequest={actionRequest} />
        </ContentWrapper>
    );
}

export default AddContentView;
