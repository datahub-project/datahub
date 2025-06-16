import {
    PUBLISH_EXPLANATION,
    PUBLISH_MODAL_TEXT,
    UNPUBLISH_EXPLANATION,
    UNPUBLISH_MODAL_TEXT,
    getStatusDetails,
    handleInputChange,
    isFormAssignmentInProgress,
    mapPromptsToCreatePromptInput,
    questionTypes,
} from '@app/govern/Dashboard/Forms/formUtils';

import { AssignmentStatus, FormPromptType, FormState } from '@types';

describe('formUtils', () => {
    describe('handleInputChange', () => {
        it('should update form values with new input', () => {
            const setFormValues = vi.fn();
            const handler = handleInputChange(setFormValues);
            const event = {
                target: {
                    id: 'formName',
                    value: 'Test Form',
                },
            };

            handler(event);

            expect(setFormValues).toHaveBeenCalledWith(expect.any(Function));
            const updateFn = setFormValues.mock.calls[0][0];
            const result = updateFn({ formName: 'Old Name' });
            expect(result).toEqual({ formName: 'Test Form' });
        });
    });

    describe('questionTypes', () => {
        it('should have all required question types', () => {
            const types = questionTypes.map((type) => type.value);
            expect(types).toContain('OWNERSHIP');
            expect(types).toContain('DOCUMENTATION');
            expect(types).toContain('FIELDS_DOCUMENTATION');
            expect(types).toContain('GLOSSARY_TERMS');
            expect(types).toContain('FIELDS_GLOSSARY_TERMS');
            expect(types).toContain('STRUCTURED_PROPERTY');
            expect(types).toContain('FIELDS_STRUCTURED_PROPERTY');
            expect(types).toContain('DOMAIN');
        });

        it('should have required fields for each type', () => {
            questionTypes.forEach((type) => {
                expect(type).toHaveProperty('label');
                expect(type).toHaveProperty('value');
                expect(type).toHaveProperty('description');
                expect(type).toHaveProperty('defaultTitle');
                expect(type).toHaveProperty('defaultDescription');
            });
        });
    });

    describe('mapPromptsToCreatePromptInput', () => {
        it('should map basic prompt fields correctly', () => {
            const prompts = [
                {
                    id: '1',
                    type: FormPromptType.Documentation,
                    title: 'Test Title',
                    description: 'Test Description',
                    required: true,
                    formUrn: 'urn:li:form:test',
                },
            ];

            const result = mapPromptsToCreatePromptInput(prompts);

            expect(result).toEqual([
                {
                    id: '1',
                    type: FormPromptType.Documentation,
                    title: 'Test Title',
                    description: 'Test Description',
                    required: true,
                },
            ]);
        });

        it('should map prompts with structured property params', () => {
            const prompts = [
                {
                    id: '1',
                    type: FormPromptType.StructuredProperty,
                    structuredPropertyParams: {
                        structuredProperty: { urn: 'urn:li:structuredProperty:test' },
                    },
                },
            ];

            const result = mapPromptsToCreatePromptInput(prompts as any);

            expect(result[0].structuredPropertyParams).toEqual({
                urn: 'urn:li:structuredProperty:test',
            });
        });

        it('should map prompts with ownership params', () => {
            const prompts = [
                {
                    id: '1',
                    type: 'OWNERSHIP',
                    ownershipParams: {
                        cardinality: 'SINGLE',
                        allowedOwners: [{ urn: 'urn:li:owner:test' }],
                        allowedOwnershipTypes: [{ urn: 'urn:li:ownershipType:test' }],
                    },
                },
            ];

            const result = mapPromptsToCreatePromptInput(prompts as any);

            expect(result[0].ownershipParams).toEqual({
                cardinality: 'SINGLE',
                allowedOwners: ['urn:li:owner:test'],
                allowedOwnershipTypes: ['urn:li:ownershipType:test'],
            });
        });

        it('should map prompts with glossary terms params', () => {
            const prompts = [
                {
                    id: '1',
                    type: 'GLOSSARY_TERMS',
                    glossaryTermsParams: {
                        cardinality: 'SINGLE',
                        allowedTerms: [{ urn: 'urn:li:glossaryTerm:test' }],
                    },
                },
            ];

            const result = mapPromptsToCreatePromptInput(prompts as any);

            expect(result[0].glossaryTermsParams).toEqual({
                cardinality: 'SINGLE',
                allowedTerms: ['urn:li:glossaryTerm:test'],
            });
        });

        it('should map prompts with domain params', () => {
            const prompts = [
                {
                    id: '1',
                    type: 'DOMAIN',
                    domainParams: {
                        allowedDomains: [{ urn: 'urn:li:domain:test' }],
                    },
                },
            ];

            const result = mapPromptsToCreatePromptInput(prompts as any);

            expect(result[0].domainParams).toEqual({
                allowedDomains: ['urn:li:domain:test'],
            });
        });
    });

    describe('getStatusDetails', () => {
        it('should return correct details for published form', () => {
            const result = getStatusDetails(FormState.Published);
            expect(result).toEqual({
                label: 'Published',
                colorScheme: 'primary',
            });
        });

        it('should return correct details for unpublished form', () => {
            const result = getStatusDetails(FormState.Unpublished);
            expect(result).toEqual({
                label: 'Unpublished',
                colorScheme: 'red',
            });
        });

        it('should return correct details for in-progress publishing', () => {
            const assignmentStatus = { status: AssignmentStatus.InProgress, timestamp: new Date().getTime() };
            const result = getStatusDetails(FormState.Published, assignmentStatus);
            expect(result).toEqual({
                label: 'Publishing',
                colorScheme: 'blue',
            });
        });

        it('should return correct details for draft form', () => {
            const result = getStatusDetails(FormState.Draft);
            expect(result).toEqual({
                label: 'Draft',
                colorScheme: 'gray',
            });
        });
    });

    describe('Constants', () => {
        it('should have correct publish explanation', () => {
            expect(PUBLISH_EXPLANATION).toBe('Publishing will assign the form to the selected assets and users.');
        });

        it('should have correct publish modal text', () => {
            expect(PUBLISH_MODAL_TEXT).toBe(
                'Are you sure? Publishing will assign the form to the selected assets and users.',
            );
        });

        it('should have correct unpublish explanation', () => {
            expect(UNPUBLISH_EXPLANATION).toBe('Unpublishing will hide this form from selected assets and users.');
        });

        it('should have correct unpublish modal text', () => {
            expect(UNPUBLISH_MODAL_TEXT).toBe(
                'Are you sure? Unpublishing will hide this form from selected assets and users.',
            );
        });
    });

    describe('isFormAssignmentInProgress', () => {
        beforeEach(() => {
            vi.useFakeTimers();
        });

        afterEach(() => {
            vi.useRealTimers();
        });

        it('should return true for in-progress assignment within last 24 hours', () => {
            const now = new Date('2024-03-20T12:00:00Z').getTime();
            vi.setSystemTime(now);

            const assignmentStatus = {
                status: AssignmentStatus.InProgress,
                timestamp: new Date('2024-03-20T11:00:00Z').getTime(), // 1 hour ago
            };

            const result = isFormAssignmentInProgress(FormState.Published, assignmentStatus);
            expect(result).toBe(true);
        });

        it('should return false for in-progress assignment older than 24 hours', () => {
            const now = new Date('2024-03-20T12:00:00Z').getTime();
            vi.setSystemTime(now);

            const assignmentStatus = {
                status: AssignmentStatus.InProgress,
                timestamp: new Date('2024-03-19T11:00:00Z').getTime(), // 25 hours ago
            };

            const result = isFormAssignmentInProgress(FormState.Published, assignmentStatus);
            expect(result).toBe(false);
        });

        it('should return false when form is not published', () => {
            const now = new Date('2024-03-20T12:00:00Z').getTime();
            vi.setSystemTime(now);

            const assignmentStatus = {
                status: AssignmentStatus.InProgress,
                timestamp: new Date('2024-03-20T11:00:00Z').getTime(),
            };

            const result = isFormAssignmentInProgress(FormState.Unpublished, assignmentStatus);
            expect(result).toBe(false);
        });

        it('should return false when assignment status is not in progress', () => {
            const now = new Date('2024-03-20T12:00:00Z').getTime();
            vi.setSystemTime(now);

            const assignmentStatus = {
                status: AssignmentStatus.Complete,
                timestamp: new Date('2024-03-20T11:00:00Z').getTime(),
            };

            const result = isFormAssignmentInProgress(FormState.Published, assignmentStatus);
            expect(result).toBe(false);
        });

        it('should return false when assignment status is null', () => {
            const result = isFormAssignmentInProgress(FormState.Published, null);
            expect(result).toBe(false);
        });

        it('should return false when assignment status is undefined', () => {
            const result = isFormAssignmentInProgress(FormState.Published, undefined);
            expect(result).toBe(false);
        });

        it('should handle missing timestamp by defaulting to 0', () => {
            const now = new Date('2024-03-20T12:00:00Z').getTime();
            vi.setSystemTime(now);

            const assignmentStatus = {
                status: AssignmentStatus.InProgress,
                // timestamp is missing
            };

            const result = isFormAssignmentInProgress(FormState.Published, assignmentStatus);
            expect(result).toBe(false);
        });
    });
});
