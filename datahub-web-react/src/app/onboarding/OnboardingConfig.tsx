import { BusinessGlossaryOnboardingConfig } from '@app/onboarding/config/BusinessGlossaryOnboardingConfig';
import { DomainsOnboardingConfig } from '@app/onboarding/config/DomainsOnboardingConfig';
import { EntityProfileOnboardingConfig } from '@app/onboarding/config/EntityProfileOnboardingConfig';
import { FormOnboardingConfig } from '@app/onboarding/config/FormOnboardingConfig';
import { GroupsOnboardingConfig } from '@app/onboarding/config/GroupsOnboardingConfig';
import { HomePageOnboardingConfig } from '@app/onboarding/config/HomePageOnboardingConfig';
import { IngestionOnboardingConfig } from '@app/onboarding/config/IngestionOnboardingConfig';
import { LineageGraphOnboardingConfig } from '@app/onboarding/config/LineageGraphOnboardingConfig';
import { PoliciesOnboardingConfig } from '@app/onboarding/config/PoliciesOnboardingConfig';
import { RolesOnboardingConfig } from '@app/onboarding/config/RolesOnboardingConfig';
import { SearchOnboardingConfig } from '@app/onboarding/config/SearchOnboardingConfig';
import { UsersOnboardingConfig } from '@app/onboarding/config/UsersOnboardingConfig';
import { ALL_V2_ONBOARDING_CONFIGS } from '@app/onboarding/configV2';
import { OnboardingStep } from '@app/onboarding/types';

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
    FormOnboardingConfig,
    ...ALL_V2_ONBOARDING_CONFIGS,
];
export const OnboardingConfig: OnboardingStep[] = ALL_ONBOARDING_CONFIGS.flat();

export const CURRENT_ONBOARDING_IDS: string[] = OnboardingConfig.map((step) => step.id as string);
