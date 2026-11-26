import React from 'react';

import { useEntityData } from '@app/entityV2/shared/EntityContext';
import CompletedView from '@app/entityV2/shared/containers/profile/sidebar/FormInfo/CompletedView';
import IncompleteView from '@app/entityV2/shared/containers/profile/sidebar/FormInfo/IncompleteView';
import useGetPromptInfo from '@app/entityV2/shared/containers/profile/sidebar/FormInfo/useGetPromptInfo';
import useIsUserAssigned from '@app/entityV2/shared/containers/profile/sidebar/FormInfo/useIsUserAssigned';
import {
    isVerificationComplete,
    shouldShowVerificationInfo,
} from '@app/entityV2/shared/containers/profile/sidebar/FormInfo/utils';

interface Props {
    formUrn?: string;
    openFormModal?: () => void;
}

export default function FormInfo({ formUrn, openFormModal }: Props) {
    const { entityData } = useEntityData();
    const { numRequiredPromptsRemaining, numOptionalPromptsRemaining } = useGetPromptInfo(formUrn);
    const showVerificationInfo = shouldShowVerificationInfo(entityData, formUrn);
    const isUserAssigned = useIsUserAssigned();
    const allRequiredPromptsAreComplete = numRequiredPromptsRemaining === 0;

    const shouldShowCompletedView = showVerificationInfo
        ? allRequiredPromptsAreComplete && isVerificationComplete(entityData, formUrn)
        : allRequiredPromptsAreComplete;

    if (shouldShowCompletedView) {
        return (
            <CompletedView
                showVerificationStyles={showVerificationInfo}
                numOptionalPromptsRemaining={numOptionalPromptsRemaining}
                isUserAssigned={isUserAssigned}
                formUrn={formUrn}
                openFormModal={openFormModal}
            />
        );
    }

    return (
        <IncompleteView
            showVerificationStyles={showVerificationInfo && !isVerificationComplete(entityData, formUrn)}
            numRequiredPromptsRemaining={numRequiredPromptsRemaining}
            numOptionalPromptsRemaining={numOptionalPromptsRemaining}
            isUserAssigned={isUserAssigned}
            openFormModal={openFormModal}
        />
    );
}
