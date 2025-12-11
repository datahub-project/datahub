/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { shouldShowVerificationPrompt } from '@app/entity/shared/entityForm/useShouldShowVerificationPrompt';

import { FormType, FormVerificationAssociation } from '@types';

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
