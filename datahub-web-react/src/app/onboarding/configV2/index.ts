import EntityProfileOnboardingConfig from '@app/onboarding/configV2/EntityProfileOnboardingConfig';
import HomePageOnboardingConfig from '@app/onboarding/configV2/HomePageOnboardingConfig';
import { OnboardingStep } from '@app/onboarding/types';

export const ALL_V2_ONBOARDING_CONFIGS: OnboardingStep[][] = [EntityProfileOnboardingConfig, HomePageOnboardingConfig];
