import { CloseOutlined, FileDoneOutlined } from '@ant-design/icons';
import React, { useContext } from 'react';
import styled from 'styled-components';
import { Button } from 'antd';
import { Tooltip } from '@components';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { PendingProposals } from './PendingProposals';
import { PendingRequests } from './PendingRequests';
import { useUserContext } from '../../../context/useUserContext';
import { useIsDocumentationFormsEnabled } from '../../../useAppConfig';
import { V2_HOME_PAGE_PENDING_TASKS_ID } from '../../../onboarding/configV2/HomePageOnboardingConfig';
import { PendingTasksSkeleton } from './PendingTasksSkeleton';
import OnboardingContext from '../../../onboarding/OnboardingContext';
import useShouldHidePendingTasks from './useShouldHidePendingTasks';

const Card = styled.div`
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 11px;
    background-color: #ffffff;
    overflow: hidden;
    padding: 16px 20px 20px 20px;
    width: 380px;
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin: 8px 0px 12px 0px;
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

const CloseButton = styled(Button)`
    margin: 0px;
    padding: 2px;
`;

const StyledCloseOutlined = styled(CloseOutlined)`
    color: ${ANTD_GRAY[8]};
    font-size: 12px;
`;

export const PendingTasks = () => {
    const {
        state: { notificationsCount, proposalCount, unfinishedTaskCount },
        loaded,
    } = useUserContext();
    const { isUserInitializing } = useContext(OnboardingContext);
    const isDocumentationFormsEnabled = useIsDocumentationFormsEnabled();
    const hidePendingTasks = useShouldHidePendingTasks();

    if (hidePendingTasks) {
        return null;
    }

    if (isUserInitializing || !loaded) {
        return (
            <Card>
                <PendingTasksSkeleton />
            </Card>
        );
    }
    // Don't show the card if there are no pending tasks
    if (unfinishedTaskCount === 0 || !unfinishedTaskCount) return null;

    // Don't show if forms are disabled and there are no proposals
    if (!isDocumentationFormsEnabled && !proposalCount) return null;

    return (
        <Card id={V2_HOME_PAGE_PENDING_TASKS_ID}>
            <Header>
                <Title>
                    <Icon /> Pending tasks
                </Title>
                <Tooltip placement="left" showArrow={false} title="Hide pending tasks">
                    <CloseButton type="text">
                        <StyledCloseOutlined />
                    </CloseButton>
                </Tooltip>
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
