import { DEFAULT_FREE_TRIAL_INSTANCE_QUESTIONS, DEFAULT_QUESTIONS } from '@app/chat/utils/suggestedQuestionsUtils';

describe('suggestedQuestionsUtils', () => {
    describe('DEFAULT_QUESTIONS', () => {
        it('is an array', () => {
            expect(Array.isArray(DEFAULT_QUESTIONS)).toBe(true);
        });

        it('contains non-empty strings', () => {
            DEFAULT_QUESTIONS.forEach((q) => {
                expect(typeof q).toBe('string');
                expect(q.length).toBeGreaterThan(0);
            });
        });

        it('has at least one question', () => {
            expect(DEFAULT_QUESTIONS.length).toBeGreaterThan(0);
        });
    });

    describe('DEFAULT_FREE_TRIAL_INSTANCE_QUESTIONS', () => {
        it('is an array', () => {
            expect(Array.isArray(DEFAULT_FREE_TRIAL_INSTANCE_QUESTIONS)).toBe(true);
        });

        it('contains non-empty strings', () => {
            DEFAULT_FREE_TRIAL_INSTANCE_QUESTIONS.forEach((q) => {
                expect(typeof q).toBe('string');
                expect(q.length).toBeGreaterThan(0);
            });
        });

        it('has at least one question', () => {
            expect(DEFAULT_FREE_TRIAL_INSTANCE_QUESTIONS.length).toBeGreaterThan(0);
        });
    });
});
