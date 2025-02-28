import React from 'react';
import styled from 'styled-components/macro';
import { ManageActorNotificationSettings } from './ManageActorNotificationSettings';

const PageContainer = styled.div`
    padding-top: 20px;
    width: 100%;
`;

const PageHeaderContainer = styled.div`
    && {
        padding-left: 24px;
    }
`;

const VerticalSpacer = styled.div`
    height: 20px;
`;

type Props = {
    isPersonal: boolean;
    groupUrn?: string;
    groupName?: string;
};

/**
 * Component used for managing actor notifications and subscriptions
 */
export const ManageActorNotifications = ({ isPersonal, groupUrn, groupName }: Props) => {
    return (
        <PageContainer>
            <PageHeaderContainer>
                <ManageActorNotificationSettings isPersonal={isPersonal} groupUrn={groupUrn} groupName={groupName} />
                <VerticalSpacer />
            </PageHeaderContainer>
        </PageContainer>
    );
};
