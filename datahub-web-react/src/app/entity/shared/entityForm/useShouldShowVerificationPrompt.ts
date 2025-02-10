import { FormType, FormVerificationAssociation } from '../../../../types.generated';
import { useEntityData } from '../EntityContext';
import useGetPromptInfo from '../containers/profile/sidebar/FormInfo/useGetPromptInfo';
import { getFormAssociation, getFormVerification } from '../containers/profile/sidebar/FormInfo/utils';

interface ShowVerificationPromptProps {
    formType?: FormType;
    numRequiredPromptsRemaining: number;
    formVerification?: FormVerificationAssociation;
}

export function shouldShowVerificationPrompt({
    formType,
    numRequiredPromptsRemaining,
    formVerification,
}: ShowVerificationPromptProps) {
    return formType === FormType.Verification && numRequiredPromptsRemaining === 0 && !formVerification;
}

/*
 * Returns whether or not we should show ther verification prompt for a given form.
 * We want to show this prompt if (1) the form is a VERIFICATION form (2) there are no more
 * require prompts remaining and either (3a) the form is not verified or (3b) it has been
 * edited more recently than the verification timestamp.
 */
export default function useShouldShowVerificationPrompt(formUrn: string) {
    const { numRequiredPromptsRemaining } = useGetPromptInfo(formUrn);
    const { entityData } = useEntityData();
    const formVerification = getFormVerification(formUrn, entityData);
    const formAssociation = getFormAssociation(formUrn, entityData);
    const formType = formAssociation?.form?.info?.type;

    return shouldShowVerificationPrompt({
        formType,
        numRequiredPromptsRemaining,
        formVerification,
    });
}
