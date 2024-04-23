import React from 'react';
import {
    isVerificationComplete,
    shouldShowVerificationInfo,
} from '../../../../../../entity/shared/containers/profile/sidebar/FormInfo/utils';
import CompletedView from './CompletedView';
import IncompleteView from './IncompleteView';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import useGetPromptInfo from '../../../../../../entity/shared/containers/profile/sidebar/FormInfo/useGetPromptInfo';
import useIsUserAssigned from '../../../../../../entity/shared/containers/profile/sidebar/FormInfo/useIsUserAssigned';

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
