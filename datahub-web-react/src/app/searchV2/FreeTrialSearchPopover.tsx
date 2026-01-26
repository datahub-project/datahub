import { CalloutCard, Icon, Text } from '@components';
import React, { useContext, useMemo } from 'react';
import styled from 'styled-components';

import { FREE_TRIAL, STEP_STATE_COMPLETE, STEP_STATE_KEY } from '@app/onboarding/configV2/FreeTrialConfig';
import { useFreeTrialPopoverVisibility } from '@app/sharedV2/freeTrial';
import { EducationStepsContext } from '@providers/EducationStepsContext';

import { useBatchUpdateStepStatesMutation } from '@graphql/step.generated';
import { StepStateResult } from '@types';

const ContentText = styled(Text)`
    color: inherit;
    line-height: inherit;
`;

/**
 * Popover component shown to free trial users on the Search page.
 * Introduces the search functionality and marks the Discover Assets step as complete.
 */
export default function FreeTrialSearchPopover() {
    const { setEducationSteps } = useContext(EducationStepsContext);
    const [batchUpdateStepStates] = useBatchUpdateStepStatesMutation();

    const stepIds = useMemo(() => [FREE_TRIAL.SEARCH_POPOVER_ID], []);
    const { isVisible, setIsVisible } = useFreeTrialPopoverVisibility({ stepIds });

    const handleClose = async () => {
        const states = [
            {
                id: FREE_TRIAL.SEARCH_POPOVER_ID,
                properties: [{ key: STEP_STATE_KEY, value: STEP_STATE_COMPLETE }],
            },
            {
                id: FREE_TRIAL.DISCOVER_ASSETS_ID,
                properties: [{ key: STEP_STATE_KEY, value: STEP_STATE_COMPLETE }],
            },
        ];

        await batchUpdateStepStates({ variables: { input: { states } } });

        const results: StepStateResult[] = [
            {
                id: FREE_TRIAL.SEARCH_POPOVER_ID,
                properties: [{ key: STEP_STATE_KEY, value: STEP_STATE_COMPLETE }],
            },
            {
                id: FREE_TRIAL.DISCOVER_ASSETS_ID,
                properties: [{ key: STEP_STATE_KEY, value: STEP_STATE_COMPLETE }],
            },
        ];
        setEducationSteps((existingSteps) => (existingSteps ? [...existingSteps, ...results] : results));

        setIsVisible(false);
    };

    if (!isVisible) {
        return null;
    }

    return (
        <CalloutCard
            icon={<Icon icon="MagnifyingGlass" source="phosphor" color="violet" size="xl" weight="fill" />}
            title="Search Assets"
            position="inline"
            primaryButtonText="Close"
            onPrimaryClick={handleClose}
            onClose={handleClose}
            showCloseButton={false}
        >
            <ContentText>Use the search bar to look up for assets in DataHub directly.</ContentText>
        </CalloutCard>
    );
}
