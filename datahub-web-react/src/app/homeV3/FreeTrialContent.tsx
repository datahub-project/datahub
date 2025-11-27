import { Button, Card, colors, Icon, Text, typography } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components';

import { FreeTrialOnboardingConfig } from '@app/onboarding/configV2/FreeTrialConfig';
import { OnboardingStep } from '@app/onboarding/types';
import PageBanner from '@app/sharedV2/PageBanner';

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const CardTitle = styled(Text)`
    font-size: ${typography.fontSizes.lg};
    font-weight: ${typography.fontWeights.bold};
    color: ${colors.gray[600]};
`;

const CardDescription = styled(Text)`
    color: ${colors.gray[1700]};
    line-height: 1.5;
`;

// Get Started Card Styles
const GetStartedCard = styled.div`
    background: ${colors.white};
    border: 1px solid ${colors.gray[100]};
    border-radius: 12px;
    padding: 20px;
`;

const GetStartedHeader = styled.div`
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 16px;
`;

const HeaderContent = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;
`;

const GetStartedTitle = styled(Text)`
    font-size: ${typography.fontSizes.lg};
    font-weight: ${typography.fontWeights.bold};
    color: ${colors.gray[600]};
`;

const GetStartedSubtitle = styled(Text)`
    color: ${colors.gray[1700]};
`;

const ProgressSection = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 20px;
`;

const ProgressLabel = styled(Text)`
    color: ${colors.gray[1700]};
    font-size: ${typography.fontSizes.sm};
`;

const ProgressCount = styled(Text)`
    color: ${colors.green[1000]};
    font-size: ${typography.fontSizes.sm};
    font-weight: ${typography.fontWeights.semiBold};
`;

const ProgressBarContainer = styled.div`
    flex: 1;
    height: 6px;
    background: ${colors.gray[100]};
    border-radius: 3px;
    margin: 0 16px;
    overflow: hidden;
`;

const ProgressBarFill = styled.div<{ $percent: number }>`
    height: 100%;
    width: ${({ $percent }) => $percent}%;
    background: linear-gradient(90deg, ${colors.violet[500]} 0%, ${colors.blue[400]} 100%);
    border-radius: 3px;
    transition: width 0.3s ease;
`;

const TaskList = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
`;

const TaskItem = styled.div<{ $isCompleted?: boolean }>`
    display: flex;
    align-items: center;
    padding: 12px 16px;
    border-radius: 8px;
    background: ${({ $isCompleted }) => ($isCompleted ? colors.gray[1500] : 'transparent')};
    opacity: ${({ $isCompleted }) => ($isCompleted ? 0.7 : 1)};
`;

const TaskIconWrapper = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 40px;
    height: 40px;
    border-radius: 8px;
    background: ${colors.violet[0]};
    margin-right: 16px;
    flex-shrink: 0;
`;

const TaskContent = styled.div`
    flex: 1;
    min-width: 0;
`;

const TaskTitle = styled(Text)`
    font-weight: ${typography.fontWeights.semiBold};
    color: ${colors.gray[600]};
    margin-bottom: 2px;
`;

const TaskDescription = styled(Text)`
    color: ${colors.gray[1700]};
    font-size: ${typography.fontSizes.sm};
    line-height: 1.4;
`;

const TaskActions = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-left: 16px;
    flex-shrink: 0;
`;

const DismissButton = styled(Text)`
    color: ${colors.gray[1700]};
    cursor: pointer;
    font-size: ${typography.fontSizes.sm};

    &:hover {
        color: ${colors.gray[600]};
    }
`;

interface TaskItemComponentProps {
    step: OnboardingStep;
    isCompleted: boolean;
    onDismiss: (id: string) => void;
    onStart: (id: string) => void;
}

const TaskItemComponent = ({ step, isCompleted, onDismiss, onStart }: TaskItemComponentProps) => {
    return (
        <TaskItem $isCompleted={isCompleted}>
            <TaskIconWrapper>
                <Icon icon={step.icon || 'Star'} color="violet" size="xl" source="phosphor" />
            </TaskIconWrapper>
            <TaskContent>
                <TaskTitle>{step.title}</TaskTitle>
                <TaskDescription>{step.content}</TaskDescription>
            </TaskContent>
            <TaskActions>
                <DismissButton onClick={() => onDismiss(step.id || '')}>Dismiss</DismissButton>
                {!isCompleted && (
                    <Button size="sm" variant="outline" onClick={() => onStart(step.id || '')}>
                        Start
                    </Button>
                )}
            </TaskActions>
        </TaskItem>
    );
};

const FreeTrialContent = () => {
    const [completedSteps, setCompletedSteps] = useState<Set<string>>(new Set());
    const [dismissedSteps, setDismissedSteps] = useState<Set<string>>(new Set());

    const handleConnectData = () => {
        // TODO: Navigate to data connection page
        console.log('Connect Your Data clicked');
    };

    const handleDismiss = (stepId: string) => {
        setDismissedSteps((prev) => new Set(prev).add(stepId));
    };

    const handleStart = (stepId: string) => {
        // TODO: Navigate to the appropriate page based on stepId
        console.log('Start clicked for:', stepId);
        // Mark as completed for demo purposes
        setCompletedSteps((prev) => new Set(prev).add(stepId));
    };

    const config = FreeTrialOnboardingConfig;
    const visibleSteps = config.steps.filter((step) => !dismissedSteps.has(step.id || ''));
    const completedCount = visibleSteps.filter((step) => completedSteps.has(step.id || '')).length;
    const totalCount = visibleSteps.length;
    const progressPercent = totalCount > 0 ? (completedCount / totalCount) * 100 : 0;

    return (
        <Container>
            <PageBanner
                icon={<Icon icon="Info" color="blue" size="lg" />}
                content={
                    <Text color="gray">
                        You&apos;re exploring DataHub with sample e-commerce data. All features are fully functional —
                        just with demo data.
                    </Text>
                }
                backgroundColor={colors.blue[0]}
                actionText="Connect Your Data"
                onAction={handleConnectData}
                actionColor={colors.blue[1000]}
            />
            <Card
                title={<CardTitle>Your organization&apos;s data catalog</CardTitle>}
                subTitle={
                    <CardDescription>
                        DataHub is like a search engine for all your data assets. Find any dataset, understand where it
                        comes from, see who uses it, and discover insights—all in one place.
                    </CardDescription>
                }
                width="100%"
                isCardClickable={false}
            />
            {visibleSteps.length > 0 && (
                <GetStartedCard>
                    <GetStartedHeader>
                        <HeaderContent>
                            <GetStartedTitle>{config.title}</GetStartedTitle>
                            <GetStartedSubtitle>{config.content}</GetStartedSubtitle>
                        </HeaderContent>
                        <Icon icon="DotsThreeVertical" source="phosphor" color="gray" size="lg" />
                    </GetStartedHeader>
                    {config.showProgress && (
                        <ProgressSection>
                            <ProgressLabel>Progress</ProgressLabel>
                            <ProgressBarContainer>
                                <ProgressBarFill $percent={progressPercent} />
                            </ProgressBarContainer>
                            <ProgressCount>
                                {completedCount} of {totalCount} tasks complete
                            </ProgressCount>
                        </ProgressSection>
                    )}
                    <TaskList>
                        {visibleSteps.map((step) => (
                            <TaskItemComponent
                                key={step.id}
                                step={step}
                                isCompleted={completedSteps.has(step.id || '')}
                                onDismiss={handleDismiss}
                                onStart={handleStart}
                            />
                        ))}
                    </TaskList>
                </GetStartedCard>
            )}
        </Container>
    );
};

export default FreeTrialContent;

