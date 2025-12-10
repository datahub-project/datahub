import { OnboardingTasks } from '@app/onboarding/types';

export const FREE_TRIAL_ONBOARDING_ID = 'free-trial-onboarding';
export const FREE_TRIAL_ONBOARDING_DATA_LINEAGE_ID = 'free-trial-onboarding-data-lineage';
export const FREE_TRIAL_ONBOARDING_ASK_DATAHUB_ID = 'free-trial-onboarding-ask-datahub';
export const FREE_TRIAL_ONBOARDING_CONNECT_SOURCE_ID = 'free-trial-onboarding-connect-source';

/**
 * Configuration for the Self serve free trial onboarding
 */
export const FreeTrialOnboardingConfig: OnboardingTasks = {
    id: FREE_TRIAL_ONBOARDING_ID,
    title: 'Get Started',
    content: 'Explore demo data on DataHub.',
    showProgress: true,
    steps: [
        {
            id: FREE_TRIAL_ONBOARDING_DATA_LINEAGE_ID,
            icon: 'TreeStructure',
            title: 'Explore Data Lineage',
            content:
                'We have cross platform lineage and column level lineage, check it out! Trace data from source to destination.',
        },
        {
            id: FREE_TRIAL_ONBOARDING_ASK_DATAHUB_ID,
            icon: 'Sparkle',
            title: 'Ask DataHub',
            content: 'Chat with our AI about the demo data like analysis, management, and discovering insights.',
        },
        {
            id: FREE_TRIAL_ONBOARDING_CONNECT_SOURCE_ID,
            icon: 'Plugs',
            title: 'Connect Your First Source',
            content:
                'Configure and schedule syncs to import data from your data sources; and set them up and keep exploring DataHub.',
        },
    ],
};

/**
 * Returns a flattened list of all IDs in FreeTrialOnboardingConfig
 * Includes the main config ID and all step IDs
 */
export const getFreeTrialOnboardingIds = (): string[] => {
    const ids: string[] = [];

    if (FreeTrialOnboardingConfig.id) {
        ids.push(FreeTrialOnboardingConfig.id);
    }

    FreeTrialOnboardingConfig.steps.forEach((step) => {
        if (step.id) {
            ids.push(step.id);
        }
    });

    return ids;
};
