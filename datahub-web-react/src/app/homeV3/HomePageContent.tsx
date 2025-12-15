import React, { useContext } from 'react';

import { Announcements } from '@app/homeV3/announcements/Announcements';
import FreeTrialContent from '@app/homeV3/freeTrial/FreeTrialContent';
import EditDefaultTemplateBar from '@app/homeV3/settings/EditDefaultTemplateBar';
import HomePageSettingsButtonWrapper from '@app/homeV3/settings/HomePageSettingsButtonWrapper';
import { CenteredContainer, ContentContainer, ContentDiv } from '@app/homeV3/styledComponents';
import Template from '@app/homeV3/template/Template';
import { FREE_TRIAL, STEP_STATE_DISMISSED, STEP_STATE_KEY } from '@app/onboarding/configV2/FreeTrialConfig';
import { getStepPropertyByKey } from '@app/onboarding/utils';
import { useIsFreeTrialInstance } from '@app/useAppConfig';
import { EducationStepsContext } from '@providers/EducationStepsContext';

const HomePageContent = () => {
    const isFreeTrialInstance = useIsFreeTrialInstance();
    const { educationSteps } = useContext(EducationStepsContext);

    // Check if the free trial onboarding card has been dismissed
    const parentState = getStepPropertyByKey(educationSteps, FREE_TRIAL.ONBOARDING_ID, STEP_STATE_KEY);
    const isOnboardingDismissed = parentState === STEP_STATE_DISMISSED;

    // Show Template if not a free trial instance OR if onboarding is dismissed
    const showFreeTrialOnboardingContent = isFreeTrialInstance && !isOnboardingDismissed;

    return (
        <ContentContainer>
            <CenteredContainer>
                <ContentDiv>
                    <HomePageSettingsButtonWrapper />
                    <Announcements />
                    {showFreeTrialOnboardingContent ? <FreeTrialContent /> : <Template />}
                    <EditDefaultTemplateBar />
                </ContentDiv>
            </CenteredContainer>
        </ContentContainer>
    );
};

export default HomePageContent;
