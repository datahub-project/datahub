import { List } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { pluralize } from '@app/shared/textUtil';
import { Button, colors } from '@src/alchemy-components';
import analytics, { DocRequestCTASource, EventType } from '@src/app/analytics';

import { FormType } from '@types';

const Title = styled.div`
    color: ${colors.gray[800]};
    font-weight: 600;
    font-size: 14px;
`;

const SubHeader = styled.div`
    color: ${colors.gray[1700]};
    font-size: 14px;
`;

type Props = {
    request: any;
    onClickOpenRequest: () => void;
};

export const RequestItem = ({ request, onClickOpenRequest }: Props) => {
    const { form, numEntitiesToComplete } = request;
    const { type, name } = form.info;

    // List of Owners
    const owners = form?.ownership?.owners;
    const isVerificationForm = type === FormType.Verification;

    // Messaging
    let message = isVerificationForm ? `New Verification Tasks` : `New Compliance Tasks`;
    if (owners && owners.length > 0) {
        const ownerName = owners[0].owner.info.displayName;
        if (ownerName)
            message = isVerificationForm
                ? `New Verification Tasks from ${ownerName}`
                : `New Compliance Tasks from ${ownerName}`;
    }

    const displayedNumEntities = numEntitiesToComplete >= 10000 ? '10,000+' : numEntitiesToComplete;

    function handleClickOpen() {
        analytics.event({ type: EventType.ClickDocRequestCTA, source: DocRequestCTASource.TaskCenter });
        onClickOpenRequest();
    }

    return (
        <>
            <List.Item key={form.urn}>
                <div>
                    <Title>{message}</Title>
                    <SubHeader>
                        Please complete {name} for{' '}
                        <b>
                            {displayedNumEntities} {pluralize(numEntitiesToComplete, 'asset')}
                        </b>
                    </SubHeader>
                </div>
                <Button variant="text" onClick={handleClickOpen}>
                    Open in Compliance Center
                </Button>
            </List.Item>
        </>
    );
};
