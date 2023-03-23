import React from 'react';
import styled from 'styled-components/macro';
import { ManageActorNotificationSettings } from './ManageActorNotificationSettings';
import { ManageActorSubscriptions } from '../subscriptions/ManageActorSubscriptions';

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
};

/**
 * Component used for managing actor notifications and subscriptions
 */
export const ManageActorNotifications = ({ isPersonal }: Props) => {
    return (
        <PageContainer>
            <PageHeaderContainer>
                <ManageActorNotificationSettings isPersonal={isPersonal} />
                <VerticalSpacer />
                <ManageActorSubscriptions isPersonal={isPersonal} />
            </PageHeaderContainer>
        </PageContainer>
    );
};
