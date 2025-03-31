import React from 'react';

import { List, Typography, Button } from 'antd';

import { FormType } from '../../../types.generated';
import { pluralize } from '../../shared/textUtil';

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

    return (
        <>
            <List.Item key={form.urn}>
                <Typography.Text>
                    <strong>{message}</strong> <br />
                    Please complete {name} for {displayedNumEntities} {pluralize(numEntitiesToComplete, 'asset')}
                </Typography.Text>
                <Button onClick={onClickOpenRequest}>Open in Compliance Center</Button>
            </List.Item>
        </>
    );
};
