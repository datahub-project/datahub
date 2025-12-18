import { CalloutCard, CalloutPosition, Icon, Text } from '@components';
import React, { useContext, useMemo } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components';

import { FREE_TRIAL, STEP_STATE_COMPLETE, STEP_STATE_KEY } from '@app/onboarding/configV2/FreeTrialConfig';
import { useFreeTrialPopoverVisibility } from '@app/sharedV2/freeTrial';
import { PageRoutes } from '@conf/Global';
import { EducationStepsContext } from '@providers/EducationStepsContext';

import { useBatchUpdateStepStatesMutation } from '@graphql/step.generated';
import { StepStateInput, StepStateResult } from '@types';

const ContentText = styled(Text)`
    color: inherit;
    line-height: inherit;
`;

export type AIChatPopoverVariant = 'welcome' | 'completion';

interface PopoverConfig {
    stepId: string;
    icon: React.ReactNode;
    title: string;
    content: string;
    position: CalloutPosition;
    primaryButtonText: string;
    navigateToHome: boolean;
}

const POPOVER_CONFIGS: Record<AIChatPopoverVariant, PopoverConfig> = {
    welcome: {
        stepId: FREE_TRIAL.AI_CHAT_POPOVER_ID,
        icon: <Icon icon="Sparkle" source="phosphor" color="violet" size="xl" weight="fill" />,
        title: 'Ask DataHub',
        content: 'Chat interface for asking questions about your data and metadata.',
        position: 'inline',
        primaryButtonText: 'Close',
        navigateToHome: false,
    },
    completion: {
        stepId: FREE_TRIAL.AI_CHAT_COMPLETION_POPOVER_ID,
        icon: <Icon icon="Sparkle" source="phosphor" color="violet" size="xl" weight="fill" />,
        title: "You've Seen Ask DataHub",
        content: 'Continue learning about the platform and our connections to your Sources.',
        position: 'fixed-top-right',
        primaryButtonText: 'Go to Home',
        navigateToHome: true,
    },
};

interface Props {
    /** Which variant of the popover to show */
    variant: AIChatPopoverVariant;
}

/**
 * Popover component shown to free trial users on the AI Chat page.
 * - 'welcome' variant: Shown on empty state, introduces the chat feature
 * - 'completion' variant: Shown after user receives a response, encourages next steps
 */
export default function FreeTrialAIChatPopover({ variant }: Props) {
    const history = useHistory();
    const config = POPOVER_CONFIGS[variant];
    const { setEducationSteps } = useContext(EducationStepsContext);
    const [batchUpdateStepStates] = useBatchUpdateStepStatesMutation();

    // Memoize stepIds array for the hook
    const stepIds = useMemo(() => [config.stepId], [config.stepId]);

    const { isVisible, setIsVisible } = useFreeTrialPopoverVisibility({ stepIds });

    const handlePrimaryClick = () => {
        const states: StepStateInput[] = [
            {
                id: config.stepId,
                properties: [],
            },
        ];

        // When completion popover is dismissed, also mark the Ask DataHub onboarding step as complete
        if (variant === 'completion') {
            states.push({
                id: FREE_TRIAL.ASK_DATAHUB_ID,
                properties: [{ key: STEP_STATE_KEY, value: STEP_STATE_COMPLETE }],
            });
        }

        batchUpdateStepStates({ variables: { input: { states } } }).then(() => {
            // Update local state to reflect the changes
            const results: StepStateResult[] = [
                {
                    id: config.stepId,
                    properties: [],
                },
            ];

            if (variant === 'completion') {
                results.push({
                    id: FREE_TRIAL.ASK_DATAHUB_ID,
                    properties: [{ key: STEP_STATE_KEY, value: STEP_STATE_COMPLETE }],
                });
            }

            setEducationSteps((existingSteps) => (existingSteps ? [...existingSteps, ...results] : results));
        });

        setIsVisible(false);
        if (config.navigateToHome) {
            history.push(PageRoutes.ROOT);
        }
    };

    if (!isVisible) {
        return null;
    }

    return (
        <CalloutCard
            icon={config.icon}
            title={config.title}
            position={config.position}
            primaryButtonText={config.primaryButtonText}
            onPrimaryClick={handlePrimaryClick}
        >
            <ContentText>{config.content}</ContentText>
        </CalloutCard>
    );
}
