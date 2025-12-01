import {
    convertStepId,
    getConditionalStepIdsToAdd,
    getInitialAllowListIds,
    getInstanceLevelOnboardingStepIds,
    getStepIds,
    getStepPropertyByKey,
    getUserSpecificStepIds,
    hasSeenPrerequisiteStepIfExists,
} from '@app/onboarding/utils';

import { StepStateResult } from '@types';

// Mock OnboardingConfig
vi.mock('@app/onboarding/OnboardingConfig', () => ({
    OnboardingConfig: [
        { id: 'step1', title: 'Step 1', isActionStep: false },
        { id: 'step2', title: 'Step 2', isActionStep: false, prerequisiteStepId: 'step1' },
        { id: 'step3', title: 'Step 3', isActionStep: true },
        { id: 'step4', title: 'Step 4', isActionStep: false },
    ],
}));

// Mock FreeTrialConfig
vi.mock('../configV2/FreeTrialConfig', () => ({
    getFreeTrialOnboardingIds: () => ['free-trial-id-1', 'free-trial-id-2'],
}));

describe('utils', () => {
    const mockUserUrn = 'urn:li:corpuser:testuser';

    describe('convertStepId', () => {
        it('should convert step ID to user-specific format', () => {
            const result = convertStepId('step1', mockUserUrn);
            expect(result).toBe(`${mockUserUrn}-step1`);
        });

        it('should return userUrn-undefined for non-existent step', () => {
            const result = convertStepId('nonexistent', mockUserUrn);
            expect(result).toBe(`${mockUserUrn}-undefined`);
        });
    });

    describe('getUserSpecificStepIds', () => {
        it('should return user-specific step IDs for all config steps', () => {
            const result = getUserSpecificStepIds(mockUserUrn);
            expect(result).toEqual([
                `${mockUserUrn}-step1`,
                `${mockUserUrn}-step2`,
                `${mockUserUrn}-step3`,
                `${mockUserUrn}-step4`,
            ]);
        });
    });

    describe('getInstanceLevelOnboardingStepIds', () => {
        it('should return free trial onboarding IDs', () => {
            const result = getInstanceLevelOnboardingStepIds();
            expect(result).toEqual(['free-trial-id-1', 'free-trial-id-2']);
        });
    });

    describe('getStepIds', () => {
        it('should return combined user-specific and instance-level step IDs', () => {
            const result = getStepIds(mockUserUrn);
            expect(result).toContain(`${mockUserUrn}-step1`);
            expect(result).toContain('free-trial-id-1');
            expect(result).toContain('free-trial-id-2');
        });
    });

    describe('getConditionalStepIdsToAdd', () => {
        it('should return conditional step IDs when prerequisite is in stepIdsToAdd', () => {
            const providedStepIds = ['step1', 'step2'];
            const stepIdsToAdd = ['step1'];

            const result = getConditionalStepIdsToAdd(providedStepIds, stepIdsToAdd);
            expect(result).toContain('step2');
        });

        it('should not return conditional step if it is already in stepIdsToAdd', () => {
            const providedStepIds = ['step1', 'step2'];
            const stepIdsToAdd = ['step1', 'step2'];

            const result = getConditionalStepIdsToAdd(providedStepIds, stepIdsToAdd);
            expect(result).not.toContain('step2');
        });

        it('should return empty array when no conditional steps need to be added', () => {
            const providedStepIds = ['step1'];
            const stepIdsToAdd = ['step3'];

            const result = getConditionalStepIdsToAdd(providedStepIds, stepIdsToAdd);
            expect(result).toEqual([]);
        });
    });

    describe('hasSeenPrerequisiteStepIfExists', () => {
        const mockEducationSteps: StepStateResult[] = [{ id: `${mockUserUrn}-step1`, properties: [] }];

        it('should return true if step has no prerequisite', () => {
            const step = { id: 'step1', title: 'Step 1' };
            const result = hasSeenPrerequisiteStepIfExists(step, mockUserUrn, mockEducationSteps);
            expect(result).toBe(true);
        });

        it('should return true if prerequisite step has been seen', () => {
            const step = { id: 'step2', title: 'Step 2', prerequisiteStepId: 'step1' };
            const result = hasSeenPrerequisiteStepIfExists(step, mockUserUrn, mockEducationSteps);
            expect(result).toBe(true);
        });

        it('should return false if prerequisite step has not been seen', () => {
            const step = { id: 'step2', title: 'Step 2', prerequisiteStepId: 'step4' };
            const result = hasSeenPrerequisiteStepIfExists(step, mockUserUrn, mockEducationSteps);
            expect(result).toBe(false);
        });
    });

    describe('getInitialAllowListIds', () => {
        it('should return IDs of steps that are not action steps', () => {
            const result = getInitialAllowListIds();
            expect(result).toContain('step1');
            expect(result).toContain('step2');
            expect(result).toContain('step4');
            expect(result).not.toContain('step3'); // step3 is an action step
        });
    });

    describe('getStepPropertyByKey', () => {
        const mockEducationSteps: StepStateResult[] = [
            {
                id: 'test-step-1',
                properties: [
                    { key: 'state', value: 'COMPLETE' },
                    { key: 'timestamp', value: '1234567890' },
                ],
            },
            {
                id: 'test-step-2',
                properties: [{ key: 'state', value: 'DISMISSED' }],
            },
            {
                id: 'test-step-3',
                properties: [],
            },
        ];

        it('should return the property value when step and key exist', () => {
            const result = getStepPropertyByKey(mockEducationSteps, 'test-step-1', 'state');
            expect(result).toBe('COMPLETE');
        });

        it('should return different property values for different keys', () => {
            const result = getStepPropertyByKey(mockEducationSteps, 'test-step-1', 'timestamp');
            expect(result).toBe('1234567890');
        });

        it('should return null when educationSteps is null', () => {
            const result = getStepPropertyByKey(null, 'test-step-1', 'state');
            expect(result).toBeNull();
        });

        it('should return null when step ID does not exist', () => {
            const result = getStepPropertyByKey(mockEducationSteps, 'nonexistent-step', 'state');
            expect(result).toBeNull();
        });

        it('should return null when property key does not exist', () => {
            const result = getStepPropertyByKey(mockEducationSteps, 'test-step-1', 'nonexistent-key');
            expect(result).toBeNull();
        });

        it('should return null when step has empty properties', () => {
            const result = getStepPropertyByKey(mockEducationSteps, 'test-step-3', 'state');
            expect(result).toBeNull();
        });

        it('should return correct value for different steps with same key', () => {
            const result1 = getStepPropertyByKey(mockEducationSteps, 'test-step-1', 'state');
            const result2 = getStepPropertyByKey(mockEducationSteps, 'test-step-2', 'state');

            expect(result1).toBe('COMPLETE');
            expect(result2).toBe('DISMISSED');
        });
    });
});
