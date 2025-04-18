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
    requestMetadataViews: {
        primary: React.ReactNode;
        additional?: React.ReactNode;
    }[];
    actionRequest: ActionRequest;
}

function AddContentView({ requestMetadataViews, actionRequest }: Props) {
    const { origin } = actionRequest;

    const renderMetadataViews = () => {
        if (!requestMetadataViews.length) return null;

        return requestMetadataViews.map((view, idx, array) => {
            const isLast = idx === array.length - 1;
            const isSecondToLast = idx === array.length - 2;

            return (
                <React.Fragment key={view.toString()}>
                    {view.primary}
                    {view.additional && <> of type {view.additional}</>}
                    {!isLast && (isSecondToLast ? ', and ' : ', ')}
                </React.Fragment>
            );
        });
    };

    return (
        <ContentWrapper>
            {origin === ActionRequestOrigin.Inferred ? (
                <AiActorLabel />
            ) : (
                <CreatedByView actionRequest={actionRequest} />
            )}
            <Typography.Text> requests to add </Typography.Text>
            {renderMetadataViews()}
            <Typography.Text>{` to `}</Typography.Text>
            <RequestTargetEntityView actionRequest={actionRequest} />
        </ContentWrapper>
    );
}

export default AddContentView;
