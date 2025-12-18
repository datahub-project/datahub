import { Button, Card, Icon, Text, colors } from '@components';
import { Dropdown, Skeleton } from 'antd';
import React, { useContext, useMemo, useState } from 'react';
import { useHistory } from 'react-router';

import {
    CardDescription,
    CardTitle,
    CompletionActions,
    CompletionContainer,
    CompletionIconWrapper,
    CompletionSubtitle,
    CompletionTitle,
    Container,
    GetStartedCard,
    GetStartedHeader,
    GetStartedSubtitle,
    GetStartedTitle,
    HeaderContent,
    MenuButton,
    MenuItem,
    ProgressBarContainer,
    ProgressBarFill,
    ProgressCount,
    ProgressHeader,
    ProgressLabel,
    ProgressSection,
    TaskList,
} from '@app/homeV3/freeTrial/FreeTrialContent.styles';
import { TaskItemComponent } from '@app/homeV3/freeTrial/TaskItemComponent';
import { useGetIngestionLink } from '@app/homeV3/freeTrial/useGetIngestionLink';
import { SYSTEM_INTERNAL_SOURCE_TYPE } from '@app/ingestV2/constants';
import {
    FREE_TRIAL,
    FreeTrialOnboardingConfig,
    STEP_STATE_COMPLETE,
    STEP_STATE_DISMISSED,
    STEP_STATE_KEY,
} from '@app/onboarding/configV2/FreeTrialConfig';
import { getStepPropertyByKey } from '@app/onboarding/utils';
import PageBanner from '@app/sharedV2/PageBanner';
import { EducationStepsContext } from '@providers/EducationStepsContext';

import { useListIngestionSourcesQuery } from '@graphql/ingestion.generated';
import { useBatchUpdateStepStatesMutation } from '@graphql/step.generated';
import { StepStateResult } from '@types';

/**
 * Component to render the Self serve free trial content
 */
const FreeTrialContent = () => {
    const history = useHistory();

    const { educationSteps, setEducationSteps } = useContext(EducationStepsContext);
    const [menuOpen, setMenuOpen] = useState(false);
    const [batchUpdateStepStates] = useBatchUpdateStepStatesMutation();

    // Query ingestion sources to check if user has connected any data sources
    const { data: ingestionSourcesData, loading: ingestionLoading } = useListIngestionSourcesQuery({
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

    // Check if we're still loading the initial data
    const isLoading = educationSteps === null || ingestionLoading;

    // Check if any ingestion source has a successful execution
    // TODO: We will need to update this to get other statuses later
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

    const ingestionLink = useGetIngestionLink(hasIngestionSources);

    // Check if the parent Get Started card should be shown
    const parentState = getStepPropertyByKey(educationSteps, FREE_TRIAL.ONBOARDING_ID, STEP_STATE_KEY);
    const isParentDismissed = parentState === STEP_STATE_DISMISSED;

    // Helper to check if a step is dismissed
    const isStepDismissed = (stepId: string): boolean => {
        const state = getStepPropertyByKey(educationSteps, stepId, STEP_STATE_KEY);
        return state === STEP_STATE_DISMISSED;
    };

    // Helper to check if a step is completed
    const isStepCompleted = (stepId: string): boolean => {
        const state = getStepPropertyByKey(educationSteps, stepId, STEP_STATE_KEY);
        if (state === STEP_STATE_COMPLETE) return true;
        // For connect source step, also check ingestion data
        if (stepId === FREE_TRIAL.CONNECT_SOURCE_ID && hasSuccessfulIngestion) return true;
        return false;
    };

    // Helper to check if a step is done (completed or dismissed)
    const isStepDone = (stepId: string): boolean => {
        return isStepCompleted(stepId) || isStepDismissed(stepId);
    };

    const handleDismissCard = () => {
        setMenuOpen(false);
        const stepState = {
            id: FREE_TRIAL.ONBOARDING_ID,
            properties: [{ key: STEP_STATE_KEY, value: STEP_STATE_DISMISSED }],
        };
        batchUpdateStepStates({ variables: { input: { states: [stepState] } } }).then(() => {
            // Update local state to reflect the change
            const result: StepStateResult = {
                id: FREE_TRIAL.ONBOARDING_ID,
                properties: [{ key: STEP_STATE_KEY, value: STEP_STATE_DISMISSED }],
            };
            setEducationSteps((existingSteps) => (existingSteps ? [...existingSteps, result] : [result]));
        });
    };

    const cardMenuItems = [
        {
            key: 'dismiss',
            label: <MenuItem onClick={handleDismissCard}>Dismiss</MenuItem>,
        },
    ];

    const config = FreeTrialOnboardingConfig;
    // Filter out done (completed or dismissed) steps for display, but keep total count based on all steps
    const visibleSteps = config.steps.filter((step) => !isStepDone(step.id || ''));
    const totalCount = config.steps.length;
    const doneCount = config.steps.filter((step) => isStepDone(step.id || '')).length;
    const progressPercent = totalCount > 0 ? (doneCount / totalCount) * 100 : 0;
    const allTasksComplete = doneCount === totalCount && totalCount > 0;

    const handleConnectData = () => {
        history.push(ingestionLink);
    };

    const handleDismiss = (stepId: string) => {
        const stepState = {
            id: stepId,
            properties: [{ key: STEP_STATE_KEY, value: STEP_STATE_DISMISSED }],
        };
        batchUpdateStepStates({ variables: { input: { states: [stepState] } } }).then(() => {
            // Update local state to reflect the change
            const result: StepStateResult = {
                id: stepId,
                properties: [{ key: STEP_STATE_KEY, value: STEP_STATE_DISMISSED }],
            };
            setEducationSteps((existingSteps) => (existingSteps ? [...existingSteps, result] : [result]));
        });
    };

    const handleStart = (stepId: string) => {
        switch (stepId) {
            case FREE_TRIAL.ASK_DATAHUB_ID:
                history.push('/ai-chat');
                break;
            case FREE_TRIAL.DATA_LINEAGE_ID:
                // Mark the step as complete when starting lineage exploration
                {
                    const stepState = {
                        id: FREE_TRIAL.DATA_LINEAGE_ID,
                        properties: [{ key: STEP_STATE_KEY, value: STEP_STATE_COMPLETE }],
                    };
                    batchUpdateStepStates({ variables: { input: { states: [stepState] } } }).then(() => {
                        const result: StepStateResult = {
                            id: FREE_TRIAL.DATA_LINEAGE_ID,
                            properties: [{ key: STEP_STATE_KEY, value: STEP_STATE_COMPLETE }],
                        };
                        setEducationSteps((existingSteps) => (existingSteps ? [...existingSteps, result] : [result]));
                    });
                }
                history.push(
                    '/dataset/urn:li:dataset:(urn:li:dataPlatform:snowflake,order_entry_db.analytics.order_details,PROD)/Lineage?highlightedPath=&is_lineage_mode=false&schemaFilter=',
                );
                break;
            case FREE_TRIAL.CONNECT_SOURCE_ID:
                history.push(ingestionLink);
                break;
            default:
                break;
        }
    };

    // Show loading skeleton while data is being fetched
    if (isLoading) {
        return (
            <Container>
                <Card
                    title={<CardTitle>Your organization&apos;s data catalog</CardTitle>}
                    subTitle={
                        <CardDescription>
                            DataHub is like a search engine for all your data assets. Find any dataset, understand where
                            it comes from, see who uses it, and discover insights, all in one place.
                        </CardDescription>
                    }
                    width="100%"
                    isCardClickable={false}
                />
                <GetStartedCard>
                    <Skeleton active paragraph={{ rows: 4 }} />
                </GetStartedCard>
            </Container>
        );
    }

    return (
        <Container>
            {!hasIngestionSources && (
                <PageBanner
                    icon={<Icon icon="Info" color="blue" size="lg" weight="fill" source="phosphor" />}
                    content={
                        <Text color="blue">
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
            {!isParentDismissed && (
                <>
                    <Card
                        title={<CardTitle>Your organization&apos;s data catalog</CardTitle>}
                        subTitle={
                            <CardDescription>
                                DataHub is like a search engine for all your data assets. Find any dataset, understand
                                where it comes from, see who uses it, and discover insights, all in one place.
                            </CardDescription>
                        }
                        width="100%"
                        isCardClickable={false}
                    />
                    <GetStartedCard>
                        <GetStartedHeader>
                            <HeaderContent>
                                <GetStartedTitle>{config.title}</GetStartedTitle>
                                {!allTasksComplete && <GetStartedSubtitle>{config.content}</GetStartedSubtitle>}
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
                                        {doneCount} of {totalCount} tasks complete
                                    </ProgressCount>
                                </ProgressHeader>
                                <ProgressBarContainer>
                                    <ProgressBarFill $percent={progressPercent} />
                                </ProgressBarContainer>
                            </ProgressSection>
                        )}
                        {allTasksComplete ? (
                            <CompletionContainer>
                                <CompletionIconWrapper>
                                    <Icon
                                        icon="Confetti"
                                        source="phosphor"
                                        color="violet"
                                        size="4xl"
                                        weight="duotone"
                                    />
                                </CompletionIconWrapper>
                                <CompletionTitle>All tasks complete!</CompletionTitle>
                                <CompletionSubtitle>
                                    Congratulations! Your platform is now fully set up and ready to use.
                                </CompletionSubtitle>
                                <CompletionActions>
                                    <Button variant="filled" onClick={handleDismissCard}>
                                        Start Customizing Home
                                    </Button>
                                </CompletionActions>
                            </CompletionContainer>
                        ) : (
                            <TaskList>
                                {visibleSteps.map((step) => (
                                    <TaskItemComponent
                                        key={step.id}
                                        step={step}
                                        onDismiss={handleDismiss}
                                        onStart={handleStart}
                                    />
                                ))}
                            </TaskList>
                        )}
                    </GetStartedCard>
                </>
            )}
        </Container>
    );
};

export default FreeTrialContent;
