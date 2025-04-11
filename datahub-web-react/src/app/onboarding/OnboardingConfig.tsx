import { BusinessGlossaryOnboardingConfig } from './config/BusinessGlossaryOnboardingConfig';
import { DomainsOnboardingConfig } from './config/DomainsOnboardingConfig';
import { EntityProfileOnboardingConfig } from './config/EntityProfileOnboardingConfig';
import { GroupsOnboardingConfig } from './config/GroupsOnboardingConfig';
import { HomePageOnboardingConfig } from './config/HomePageOnboardingConfig';
import { IngestionOnboardingConfig } from './config/IngestionOnboardingConfig';
import { LineageGraphOnboardingConfig } from './config/LineageGraphOnboardingConfig';
import { PoliciesOnboardingConfig } from './config/PoliciesOnboardingConfig';
import { RolesOnboardingConfig } from './config/RolesOnboardingConfig';
import { SearchOnboardingConfig } from './config/SearchOnboardingConfig';
import { UsersOnboardingConfig } from './config/UsersOnboardingConfig';
import { ALL_V2_ONBOARDING_CONFIGS } from './configV2';
import { OnboardingStep } from './OnboardingStep';

const ALL_ONBOARDING_CONFIGS: OnboardingStep[][] = [
    HomePageOnboardingConfig,
    SearchOnboardingConfig,
    EntityProfileOnboardingConfig,
    IngestionOnboardingConfig,
    BusinessGlossaryOnboardingConfig,
    DomainsOnboardingConfig,
    UsersOnboardingConfig,
    GroupsOnboardingConfig,
    RolesOnboardingConfig,
    PoliciesOnboardingConfig,
    LineageGraphOnboardingConfig,
    ...ALL_V2_ONBOARDING_CONFIGS,
];
export const OnboardingConfig: OnboardingStep[] = ALL_ONBOARDING_CONFIGS.reduce(
    (acc, config) => [...acc, ...config],
    [],
);

export const CURRENT_ONBOARDING_IDS: string[] = OnboardingConfig.map((step) => step.id as string);
