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
    content: 'Explore sample data on DataHub.',
    showProgress: true,
    steps: [
        {
            id: FREE_TRIAL.DATA_LINEAGE_ID,
            icon: 'TreeStructure',
            title: 'Explore Data Lineage',
            content: 'Visually discover connections across your data ecosystem, including at the column-level.',
        },
        {
            id: FREE_TRIAL.ASK_DATAHUB_ID,
            icon: 'Sparkle',
            title: 'Ask DataHub',
            content:
                'Use our AI assistant to find, govern, observe, and build data with simple prompts and interactive questions.',
        },
        {
            id: FREE_TRIAL.CONNECT_SOURCE_ID,
            icon: 'Plugs',
            title: 'Connect Your First Source',
            content: 'Bring your own data into DataHub by connecting your sources and running ingestion.',
        },
    ],
};

/**
 * Returns all free trial IDs
 */
export const getFreeTrialOnboardingIds = (): string[] => Object.values(FREE_TRIAL);
