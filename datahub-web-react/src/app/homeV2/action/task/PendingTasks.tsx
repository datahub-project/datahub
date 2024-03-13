import { FileDoneOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { PendingProposals } from './PendingProposals';
// import { PendingRequests } from './PendingRequests';
import { useGetPendingDocumentationProposals } from './useGetPendingDocumentationProposals';
import { useGetPendingDocumentationRequests } from './useGetPendingDocumentationRequests';

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
    const { count: documentationRequestCount /* loading: documentationRequestsLoading */ } =
        useGetPendingDocumentationRequests();
    const { count: documentationProposalCount, loading: documentationProposalsLoading } =
        useGetPendingDocumentationProposals();

    if (!documentationRequestCount /* && !documentationProposalCount */) {
        // Confirm that we want to hide the module when you don't have pending tasks.
        return null;
    }

    return (
        <Card>
            <Header>
                <Title>
                    <Icon /> Pending tasks
                </Title>
            </Header>
            <Section>
                {(documentationProposalCount && (
                    <PendingProposals count={documentationProposalCount} loading={documentationProposalsLoading} />
                )) ||
                    null}
                {/* {(documentationRequestCount && (
                    <PendingRequests count={documentationRequestCount} loading={documentationRequestsLoading} />
                )) ||
                    null} */}
            </Section>
        </Card>
    );
};
