import { Text } from '@components';
import { ActionRequest, ActionRequestOrigin } from '@src/types.generated';
import React from 'react';
import styled from 'styled-components';
import AiActorLabel from './AiActorLabel';
import CreatedByView from './CreatedByView';
import RequestTargetEntityView from './RequestTargetEntityView';
import { ContentWrapper } from './styledComponents';

const CenteredText = styled(Text)`
    display: inline-flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;

    span {
        margin-bottom: 0 !important;
    }
`;

const Comma = styled.span`
    margin-left: -4px;
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
                    {!isLast && (isSecondToLast ? 'and ' : <Comma>,</Comma>)}
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
            <CenteredText color="gray" type="span" weight="medium">
                requests to add {renderMetadataViews()}
                {` to `}
            </CenteredText>
            <RequestTargetEntityView actionRequest={actionRequest} />
        </ContentWrapper>
    );
}

export default AddContentView;
