import { FileDoneOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { PendingProposals } from './PendingProposals';
import { PendingRequests } from './PendingRequests';
import { useUserContext } from '../../../context/useUserContext';
import { useIsDocumentationFormsEnabled } from '../../../useAppConfig';

const Card = styled.div`
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 11px;
    background-color: #ffffff;
    overflow: hidden;
    padding: 16px 20px 20px 20px;
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    margin: 8px 0px 20px 0px;
`;

const Title = styled.div`
    font-weight: 600;
    font-size: 14px;
    color: #434863;
    word-break: break-word;
    display: flex;
    align-items: center;
`;

const Icon = styled(FileDoneOutlined)`
    margin-right: 8px;
    color: #9884d4;
    font-size: 16px;
`;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 8px;
`;

export const PendingTasks = () => {
    const {
        state: { notificationsCount, proposalCount, unfinishedTaskCount },
    } = useUserContext();
    const isDocumentationFormsEnabled = useIsDocumentationFormsEnabled();

    // Don't show the card if there are no pending tasks
    if (unfinishedTaskCount === 0 || !unfinishedTaskCount) return null;

    // Don't show if forms are disabled and there are no proposals
    if (!isDocumentationFormsEnabled && !proposalCount) return null;

    return (
        <Card>
            <Header>
                <Title>
                    <Icon /> Pending tasks
                </Title>
            </Header>
            <Section>
                {proposalCount > 0 && <PendingProposals count={proposalCount} />}
                {notificationsCount > 0 && isDocumentationFormsEnabled && (
                    <PendingRequests count={notificationsCount} />
                )}
            </Section>
        </Card>
    );
};
