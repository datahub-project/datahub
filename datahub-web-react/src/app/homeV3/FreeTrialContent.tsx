import { Button, Card, colors, Icon, Text, typography } from '@components';
import { Dropdown } from 'antd';
import React, { useContext, useMemo, useState } from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { SYSTEM_INTERNAL_SOURCE_TYPE } from '@app/ingestV2/constants';
import {
    FREE_TRIAL_ONBOARDING_CONNECT_SOURCE_ID,
    FREE_TRIAL_ONBOARDING_ID,
    FreeTrialOnboardingConfig,
} from '@app/onboarding/configV2/FreeTrialConfig';
import { OnboardingStep } from '@app/onboarding/types';
import PageBanner from '@app/sharedV2/PageBanner';
import { PageRoutes } from '@conf/Global';
import { EducationStepsContext } from '@providers/EducationStepsContext';

import { useListIngestionSourcesQuery } from '@graphql/ingestion.generated';
import { StepStateResult } from '@types';

// Step state constants
const STEP_STATE_DISMISSED = 'DISMISSED';
const STEP_STATE_COMPLETE = 'COMPLETE';

/**
 * Helper to get a step's state from educationSteps
 */
const getStepState = (educationSteps: StepStateResult[] | null, stepId: string): string | null => {
    if (!educationSteps) return null;
    const stepResult = educationSteps.find((step) => step.id === stepId);
    if (!stepResult) return null;
    const stateEntry = stepResult.properties.find((prop) => prop.key === 'state');
    return stateEntry?.value || null;
};

const Container = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
    padding: 0 42px;
`;

const CardTitle = styled(Text)`
    font-size: ${typography.fontSizes.lg};
    font-weight: ${typography.fontWeights.bold};
    color: ${colors.gray[600]};
`;

const CardDescription = styled(Text)`
    color: ${colors.gray[1700]};
    line-height: 1.5;
    margin-top: 16px;
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
    flex-direction: column;
    gap: 8px;
    margin-bottom: 20px;
`;

const ProgressHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
`;

const ProgressLabel = styled(Text)`
    color: ${colors.gray[1700]};
    font-size: ${typography.fontSizes.sm};
    font-weight: ${typography.fontWeights.semiBold};
`;

const ProgressCount = styled(Text)`
    color: ${colors.gray[1700]};
    font-size: ${typography.fontSizes.sm};
    font-weight: ${typography.fontWeights.semiBold};
`;

const ProgressBarContainer = styled.div`
    width: 100%;
    height: 6px;
    background: ${colors.gray[100]};
    border-radius: 3px;
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
    padding: ${({ $isCompleted }) => ($isCompleted ? '12px 16px' : '12px 0')};
    margin: ${({ $isCompleted }) => ($isCompleted ? '0 -16px' : '0')};
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
    border-radius: 200px;
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

const MenuButton = styled(Button)`
    padding: 4px;
`;

const MenuItem = styled.div`
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 2px 12px;
    cursor: pointer;
    color: ${colors.gray[600]};
    font-size: ${typography.fontSizes.md};

    &:hover {
        background: ${colors.gray[100]};
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
    const history = useHistory();
    const { educationSteps } = useContext(EducationStepsContext);
    const [menuOpen, setMenuOpen] = useState(false);

    // Query ingestion sources to check if user has connected any data sources
    const { data: ingestionSourcesData } = useListIngestionSourcesQuery({
        variables: {
            input: {
                start: 0,
                count: 25,
                filters: [
                    {
                        field: 'sourceType',
                        values: [SYSTEM_INTERNAL_SOURCE_TYPE],
                        negated: true,
                    },
                ],
            },
        },
        fetchPolicy: 'cache-and-network',
    });

    // Check if any ingestion source has a successful execution
    const hasSuccessfulIngestion = useMemo(() => {
        const sources = ingestionSourcesData?.listIngestionSources?.ingestionSources || [];
        return sources.some((source) => {
            const latestExecution = source?.executions?.executionRequests?.[0];
            const status = latestExecution?.result?.status;
            return status === 'SUCCESS' || status === 'SUCCEEDED_WITH_WARNINGS';
        });
    }, [ingestionSourcesData]);

    // Check if user has any ingestion sources configured (regardless of status)
    const hasIngestionSources = useMemo(() => {
        const total = ingestionSourcesData?.listIngestionSources?.total || 0;
        return total > 0;
    }, [ingestionSourcesData]);

    // Check if the parent Get Started card should be shown
    const parentState = getStepState(educationSteps, FREE_TRIAL_ONBOARDING_ID);
    const isParentDismissed = parentState === STEP_STATE_DISMISSED;

    // Helper to check if a step is dismissed
    const isStepDismissed = (stepId: string): boolean => {
        const state = getStepState(educationSteps, stepId);
        return state === STEP_STATE_DISMISSED;
    };

    // Helper to check if a step is completed
    const isStepCompleted = (stepId: string): boolean => {
        const state = getStepState(educationSteps, stepId);
        if (state === STEP_STATE_COMPLETE) return true;
        // For connect source step, also check ingestion data
        if (stepId === FREE_TRIAL_ONBOARDING_CONNECT_SOURCE_ID && hasSuccessfulIngestion) return true;
        return false;
    };

    const handleConnectData = () => {
        history.push(PageRoutes.INGESTION);
    };

    const handleDismissCard = () => {
        // TODO: Call API to update FREE_TRIAL_ONBOARDING_ID state to DISMISSED
        setMenuOpen(false);
        console.log('Dismiss card clicked for:', FREE_TRIAL_ONBOARDING_ID);
    };

    const cardMenuItems = [
        {
            key: 'dismiss',
            label: (
                <MenuItem onClick={handleDismissCard}>
                    Dismiss
                </MenuItem>
            ),
        },
    ];

    const handleDismiss = (stepId: string) => {
        // TODO: Call API to update step state to DISMISSED
        console.log('Dismiss clicked for:', stepId);
    };

    const handleStart = (stepId: string) => {
        // TODO: Navigate to the appropriate page based on stepId
        console.log('Start clicked for:', stepId);
    };

    const config = FreeTrialOnboardingConfig;
    // Filter out dismissed steps
    const visibleSteps = config.steps.filter((step) => !isStepDismissed(step.id || ''));
    const completedCount = visibleSteps.filter((step) => isStepCompleted(step.id || '')).length;
    const totalCount = visibleSteps.length;
    const progressPercent = totalCount > 0 ? (completedCount / totalCount) * 100 : 0;

    return (
        <Container>
            {hasIngestionSources && (
                <PageBanner
                    icon={<Icon icon="Info" color="blue" size="lg" weight="fill" source="phosphor" />}
                    content={
                        <Text color="gray">
                            You&apos;re exploring DataHub with sample e-commerce data. All features are fully functional
                            with demo data.
                        </Text>
                    }
                    backgroundColor={colors.blue[0]}
                    actionText="Connect Your Data"
                    onAction={handleConnectData}
                    actionColor={colors.blue[1000]}
                />
            )}
            <Card
                title={<CardTitle>Your organization&apos;s data catalog</CardTitle>}
                subTitle={
                    <CardDescription>
                        DataHub is like a search engine for all your data assets. Find any dataset, understand where it
                        comes from, see who uses it, and discover insights, all in one place.
                    </CardDescription>
                }
                width="100%"
                isCardClickable={false}
            />
            {!isParentDismissed && visibleSteps.length > 0 && (
                <GetStartedCard>
                    <GetStartedHeader>
                        <HeaderContent>
                            <GetStartedTitle>{config.title}</GetStartedTitle>
                            <GetStartedSubtitle>{config.content}</GetStartedSubtitle>
                        </HeaderContent>
                        <Dropdown
                            menu={{ items: cardMenuItems }}
                            trigger={['click']}
                            open={menuOpen}
                            onOpenChange={setMenuOpen}
                        >
                            <MenuButton variant="text">
                                <Icon icon="DotsThreeVertical" source="phosphor" color="gray" size="lg" />
                            </MenuButton>
                        </Dropdown>
                    </GetStartedHeader>
                    {config.showProgress && (
                        <ProgressSection>
                            <ProgressHeader>
                                <ProgressLabel>Progress</ProgressLabel>
                                <ProgressCount>
                                    {completedCount} of {totalCount} tasks complete
                                </ProgressCount>
                            </ProgressHeader>
                            <ProgressBarContainer>
                                <ProgressBarFill $percent={progressPercent} />
                            </ProgressBarContainer>
                        </ProgressSection>
                    )}
                    <TaskList>
                        {visibleSteps.map((step) => (
                            <TaskItemComponent
                                key={step.id}
                                step={step}
                                isCompleted={isStepCompleted(step.id || '')}
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

