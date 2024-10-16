import { FormType, FormVerificationAssociation } from '../../../../../types.generated';
import { shouldShowVerificationPrompt } from '../useShouldShowVerificationPrompt';

describe('shouldShowVerificationPrompt', () => {
    const formVerification = {
        form: { urn: 'urn:li:form:1' },
        lastModified: { time: 100 },
    } as FormVerificationAssociation;

    it('should return true if the form is verification, there are no prompts remaining, and no verification', () => {
        const shouldShow = shouldShowVerificationPrompt({
            formType: FormType.Verification,
            numRequiredPromptsRemaining: 0,
        });

        expect(shouldShow).toBe(true);
    });

    it('should return false if the form was verified', () => {
        const shouldShow = shouldShowVerificationPrompt({
            formType: FormType.Verification,
            numRequiredPromptsRemaining: 0,
            formVerification,
        });

        expect(shouldShow).toBe(false);
    });

    it('should return false if the form is not of type verification', () => {
        const shouldShow = shouldShowVerificationPrompt({
            formType: FormType.Completion,
            numRequiredPromptsRemaining: 0,
            formVerification,
        });

        expect(shouldShow).toBe(false);
    });

    it('should return false if the form has prompts remaining', () => {
        const shouldShow = shouldShowVerificationPrompt({
            formType: FormType.Verification,
            numRequiredPromptsRemaining: 1,
            formVerification,
        });

        expect(shouldShow).toBe(false);
    });
});
