import { Card, Icon, Text, colors } from '@components';
import { Dropdown } from 'antd';
import React, { useContext, useMemo, useState } from 'react';
import { useHistory } from 'react-router';

import {
    CardDescription,
    CardTitle,
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
import { SYSTEM_INTERNAL_SOURCE_TYPE } from '@app/ingestV2/constants';
import {
    FREE_TRIAL_ONBOARDING_CONNECT_SOURCE_ID,
    FREE_TRIAL_ONBOARDING_ID,
    FreeTrialOnboardingConfig,
} from '@app/onboarding/configV2/FreeTrialConfig';
import { getStepPropertyByKey } from '@app/onboarding/utils';
import PageBanner from '@app/sharedV2/PageBanner';
import { PageRoutes } from '@conf/Global';
import { EducationStepsContext } from '@providers/EducationStepsContext';

import { useListIngestionSourcesQuery } from '@graphql/ingestion.generated';

// Step state constants
const STEP_STATE_DISMISSED = 'DISMISSED';
const STEP_STATE_COMPLETE = 'COMPLETE';
const STEP_STATE_KEY = 'state';

/**
 * Component to render the Self serve free trial content
 */
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

    // Check if the parent Get Started card should be shown
    const parentState = getStepPropertyByKey(educationSteps, FREE_TRIAL_ONBOARDING_ID, STEP_STATE_KEY);
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
        if (stepId === FREE_TRIAL_ONBOARDING_CONNECT_SOURCE_ID && hasSuccessfulIngestion) return true;
        return false;
    };

    const handleDismissCard = () => {
        // TODO: Call API to update FREE_TRIAL_ONBOARDING_ID state to DISMISSED
        setMenuOpen(false);
        console.log('Dismiss card clicked for:', FREE_TRIAL_ONBOARDING_ID);
    };

    const cardMenuItems = [
        {
            key: 'dismiss',
            label: <MenuItem onClick={handleDismissCard}>Dismiss</MenuItem>,
        },
    ];

    const config = FreeTrialOnboardingConfig;
    // Filter out dismissed steps
    const visibleSteps = config.steps.filter((step) => !isStepDismissed(step.id || ''));
    const completedCount = visibleSteps.filter((step) => isStepCompleted(step.id || '')).length;
    const totalCount = visibleSteps.length;
    const progressPercent = totalCount > 0 ? (completedCount / totalCount) * 100 : 0;

    const handleConnectData = () => {
        history.push(PageRoutes.INGESTION);
    };

    const handleDismiss = (stepId: string) => {
        // TODO: Call API to update step state to DISMISSED
        console.log('Dismiss clicked for:', stepId);
    };

    const handleStart = (stepId: string) => {
        // TODO: Navigate to the appropriate page based on stepId
        console.log('Start clicked for:', stepId);
    };

    return (
        <Container>
            {!hasIngestionSources && (
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
