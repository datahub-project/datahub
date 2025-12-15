import { OnboardingTasks } from '@app/onboarding/types';

// Step state constants used for tracking onboarding progress
export const STEP_STATE_KEY = 'state';
export const STEP_STATE_COMPLETE = 'COMPLETE';
export const STEP_STATE_DISMISSED = 'DISMISSED';

export const FREE_TRIAL = {
    ONBOARDING_ID: 'free-trial-onboarding',
    DATA_LINEAGE_ID: 'free-trial-onboarding-data-lineage',
    ASK_DATAHUB_ID: 'free-trial-onboarding-ask-datahub',
    CONNECT_SOURCE_ID: 'free-trial-onboarding-connect-source',
    // Lineage Tour step IDs
    LINEAGE_TOUR_STEP_1_ID: 'free-trial-lineage-tour-step-1',
    LINEAGE_TOUR_STEP_2_ID: 'free-trial-lineage-tour-step-2',
    LINEAGE_TOUR_STEP_3_ID: 'free-trial-lineage-tour-step-3',
    LINEAGE_TOUR_STEP_4_ID: 'free-trial-lineage-tour-step-4',
    // AI Chat Tour step IDs
    AI_CHAT_POPOVER_ID: 'free-trial-ai-chat-popover',
    AI_CHAT_COMPLETION_POPOVER_ID: 'free-trial-ai-chat-completion-popover',
} as const;

/**
 * Configuration for the Self serve free trial onboarding
 */
export const FreeTrialOnboardingConfig: OnboardingTasks = {
    id: FREE_TRIAL.ONBOARDING_ID,
    title: 'Get Started',
    content: 'Explore demo data on DataHub.',
    showProgress: true,
    steps: [
        {
            id: FREE_TRIAL.DATA_LINEAGE_ID,
            icon: 'TreeStructure',
            title: 'Explore Data Lineage',
            content:
                'We have cross platform lineage and column level lineage, check it out! Trace data from source to destination.',
        },
        {
            id: FREE_TRIAL.ASK_DATAHUB_ID,
            icon: 'Sparkle',
            title: 'Ask DataHub',
            content: 'Chat with our AI about the demo data like analysis, management, and discovering insights.',
        },
        {
            id: FREE_TRIAL.CONNECT_SOURCE_ID,
            icon: 'Plugs',
            title: 'Connect Your First Source',
            content:
                'Configure and schedule syncs to import data from your data sources; and set them up and keep exploring DataHub.',
        },
    ],
};

/**
 * Returns all free trial IDs
 */
export const getFreeTrialOnboardingIds = (): string[] => Object.values(FREE_TRIAL);
