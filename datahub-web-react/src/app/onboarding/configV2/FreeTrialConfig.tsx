import { OnboardingTasks } from '@app/onboarding/types';

export const FREE_TRIAL_ONBOARDING_ID = 'free-trial-get-started';

export const FreeTrialOnboardingConfig: OnboardingTasks = {
    id: FREE_TRIAL_ONBOARDING_ID,
    title: 'Get Started',
    content: 'Explore demo data on DataHub.',
    showProgress: true,
    steps: [
        {
            id: 'explore-data-lineage',
            icon: 'GraphOut',
            title: 'Explore Data Lineage',
            content:
                'We have cross platform lineage and column level lineage, check it out! Trace data from source to destination.',
        },
        {
            id: 'ask-datahub',
            icon: 'Magic',
            title: 'Ask DataHub',
            content: 'Chat with our AI about the demo data like analysis, management, and discovering insights.',
        },
        {
            id: 'connect-first-source',
            icon: 'Plugs',
            title: 'Connect Your First Source',
            content:
                'Configure and schedule syncs to import data from your data sources; and set them up and keep exploring DataHub.',
        },
    ],
};

