import { isAssignedToForm } from '@app/entity/shared/containers/profile/sidebar/FormInfo/useIsUserAssigned';
import {
    getEntityPromptsInfo,
    getFieldPromptsInfo,
    getFormAssociations,
    getNumEntityPromptsRemaining,
    getNumPromptsCompletedForField,
    getNumSchemaFieldPromptsRemaining,
    getPromptsForForm,
    getVerificationAuditStamp,
    isVerificationComplete,
    shouldShowVerificationInfo,
} from '@app/entity/shared/containers/profile/sidebar/FormInfo/utils';
import { mockEntityData, mockEntityDataAllVerified } from '@app/entity/shared/entityForm/mocks';

import { FormAssociation, FormPrompt } from '@types';

// only looking at IDs
const prompts = [{ id: '1' }, { id: '2' }, { id: '3' }, { id: '4' }] as FormPrompt[];

describe('form prompt utils', () => {
    it('should get the correct number of top-level prompts remaining', () => {
        const numPromptsRemaining = getNumEntityPromptsRemaining(prompts, mockEntityData);
        expect(numPromptsRemaining).toBe(2);
    });

    // if there are 2 top level prompts for schema fields, 8 fields in the schema, then there are 16 total schema-field prompts
    // there are 5 completed prompts in the mock data, should have 11 remaining
    it('should get the correct number of field-level prompts remaining', () => {
        const fieldFormPrompts = [{ id: '1' }, { id: '2' }] as FormPrompt[];
        const numPromptsRemaining = getNumSchemaFieldPromptsRemaining(mockEntityData, fieldFormPrompts, 8);
        expect(numPromptsRemaining).toBe(11);
    });

    it('should get the correct number of field-level prompts remaining given a form urn', () => {
        const fieldFormPrompts = [{ id: '1' }, { id: '2' }] as FormPrompt[];
        const numPromptsRemaining = getNumSchemaFieldPromptsRemaining(
            mockEntityData,
            fieldFormPrompts,
            8,
            'urn:li:form:1',
        );
        expect(numPromptsRemaining).toBe(11);
    });

    it('should get the correct number of field-level prompts remaining given a form urn with no field level prompts completed', () => {
        const fieldFormPrompts = [{ id: '3' }] as FormPrompt[];
        const numPromptsRemaining = getNumSchemaFieldPromptsRemaining(
            mockEntityData,
            fieldFormPrompts,
            8,
            'urn:li:form:2',
        );
        // none are completed in this form, with only 1 schema field prompt with 8 schema fields, so all 8 should be remaining
        expect(numPromptsRemaining).toBe(8);
    });

    it('should get the numer of completed prompts for a given schema field in incompletePrompts', () => {
        const numCompleted = getNumPromptsCompletedForField('test2', mockEntityData, 'urn:li:form:1');
        expect(numCompleted).toBe(1);
    });

    it('should get the numer of completed prompts for a given schema field in completedPrompts and incompletePrompts', () => {
        const numCompleted = getNumPromptsCompletedForField('test3', mockEntityData, 'urn:li:form:1');
        expect(numCompleted).toBe(2);
    });

    it('should get the prompts for a given form urn correctly', () => {
        const promptsForForm = getPromptsForForm('urn:li:form:1', mockEntityData);
        expect(promptsForForm.length).toBe(2);
        expect(promptsForForm.map((p) => p.id)).toMatchObject(['1', '2']);
    });

    it('should get information for entity specific prompts', () => {
        const promptsForForm = getPromptsForForm('urn:li:form:2', mockEntityData);
        const { entityPrompts, numOptionalEntityPromptsRemaining, numRequiredEntityPromptsRemaining } =
            getEntityPromptsInfo(promptsForForm, mockEntityData);

        expect(entityPrompts.length).toBe(2);
        expect(entityPrompts.map((p) => p.id)).toMatchObject(['3', '5']);
        expect(numOptionalEntityPromptsRemaining).toBe(1);
        expect(numRequiredEntityPromptsRemaining).toBe(0);
    });

    it('should get information for field specific prompts', () => {
        const promptsForForm = getPromptsForForm('urn:li:form:1', mockEntityData);
        const { fieldPrompts, numOptionalFieldPromptsRemaining, numRequiredFieldPromptsRemaining } =
            getFieldPromptsInfo(promptsForForm, mockEntityData, 8, 'urn:li:form:1');

        expect(fieldPrompts.length).toBe(2);
        expect(fieldPrompts.map((p) => p.id)).toMatchObject(['1', '2']);
        expect(numOptionalFieldPromptsRemaining).toBe(11);
        expect(numRequiredFieldPromptsRemaining).toBe(0);
    });

    it('should get all form associations for an entity', () => {
        const formAssociations = getFormAssociations(mockEntityData);
        expect(formAssociations.length).toBe(3);
        expect(formAssociations.map((f) => f.form.urn)).toMatchObject([
            'urn:li:form:1',
            'urn:li:form:2',
            'urn:li:form:3',
        ]);
    });
});

describe('useIsUserAssigned utils tests', () => {
    const TECHNICAL_OWNER = 'urn:li:ownershipType:__system__technical_owner';
    const DATA_STEWARD = 'urn:li:ownershipType:__system__data_steward';
    const BUSINESS_OWNER = 'urn:li:ownershipType:__system__business_owner';

    const mockOwners = [
        { owner: { urn: 'urn:li:corpuser:user1' }, ownershipType: { urn: TECHNICAL_OWNER } },
        { owner: { urn: 'urn:li:corpuser:user2' }, ownershipType: { urn: DATA_STEWARD } },
    ];
    const userUrn = 'urn:li:corpuser:user1';

    function withOwnershipTypes(typeUrns: string[] | undefined): FormAssociation {
        const base = mockEntityData.forms?.incompleteForms[0];
        if (!base) throw new Error('mockEntityData missing incomplete form');
        return {
            ...base,
            form: {
                ...base.form,
                info: {
                    ...base.form.info,
                    actors: {
                        ...base.form.info.actors,
                        ownershipTypes: typeUrns?.map((urn) => ({ urn })),
                    },
                },
            },
        } as FormAssociation;
    }

    it('returns true when user is an owner and the form is assigned to owners', () => {
        const form = mockEntityData.forms?.incompleteForms[0] as FormAssociation;
        expect(isAssignedToForm(form, mockOwners, userUrn)).toBe(true);
    });

    it('returns false when user is not an owner and the form is assigned to owners', () => {
        const form = mockEntityData.forms?.incompleteForms[0] as FormAssociation;
        expect(isAssignedToForm(form, [], 'urn:li:corpuser:user3')).toBe(false);
    });

    it('returns true when the user is explicitly assigned', () => {
        const form = mockEntityData.forms?.completedForms[0] as FormAssociation;
        expect(isAssignedToForm(form, [], 'urn:li:corpuser:user2')).toBe(true);
    });

    it('respects ownership type filter when set', () => {
        const matching = withOwnershipTypes([TECHNICAL_OWNER]);
        const nonMatching = withOwnershipTypes([BUSINESS_OWNER]);
        expect(isAssignedToForm(matching, mockOwners, userUrn)).toBe(true);
        expect(isAssignedToForm(nonMatching, mockOwners, userUrn)).toBe(false);
    });

    it('matches when at least one of multiple ownership types is satisfied', () => {
        const form = withOwnershipTypes([TECHNICAL_OWNER, DATA_STEWARD]);
        expect(isAssignedToForm(form, mockOwners, 'urn:li:corpuser:user2')).toBe(true);
    });

    it('treats undefined and empty ownership types as "any owner"', () => {
        expect(isAssignedToForm(withOwnershipTypes(undefined), mockOwners, userUrn)).toBe(true);
        expect(isAssignedToForm(withOwnershipTypes([]), mockOwners, userUrn)).toBe(true);
    });
});

describe('shouldShowVerificationInfo', () => {
    it('should return true if a form is supplied that is a verification form', () => {
        const showVerificationInfo = shouldShowVerificationInfo(mockEntityData, 'urn:li:form:1');
        expect(showVerificationInfo).toBe(true);
    });

    it('should return false if a form is supplied that is not a verification form', () => {
        const isAssigned = shouldShowVerificationInfo(mockEntityData, 'urn:li:form:3');
        expect(isAssigned).toBe(false);
    });

    it('should return true if no formUrn is supplied and there is a verification form', () => {
        const isAssigned = shouldShowVerificationInfo(mockEntityData);
        expect(isAssigned).toBe(true);
    });
});

describe('getVerificationAuditStamp', () => {
    it('should return the audit stamp for a given form', () => {
        const auditStamp = getVerificationAuditStamp(mockEntityData, 'urn:li:form:2');
        expect(auditStamp).toMatchObject({
            actor: {
                urn: 'urn:li:corpuser:test',
            },
            time: 100,
        });
    });

    it('should return undefined for audit stamp for a given form with no verifications', () => {
        const auditStamp = getVerificationAuditStamp(mockEntityData, 'urn:li:form:1');
        expect(auditStamp).toBe(null);
    });

    it('should return the most recent audit stamp when not given form', () => {
        const auditStamp = getVerificationAuditStamp(mockEntityData);
        expect(auditStamp).toMatchObject({
            actor: {
                urn: 'urn:li:corpuser:test',
            },
            time: 101,
        });
    });
});

describe('isVerificationComplete', () => {
    it('should return true if the given form is verified', () => {
        const isComplete = isVerificationComplete(mockEntityData, 'urn:li:form:2');
        expect(isComplete).toBe(true);
    });

    it('should return false if the given form is not verified', () => {
        const isComplete = isVerificationComplete(mockEntityData, 'urn:li:form:1');
        expect(isComplete).toBe(false);
    });

    it('should return false if no form is given and not all verification forms are complete', () => {
        const isComplete = isVerificationComplete(mockEntityData);
        expect(isComplete).toBe(false);
    });

    it('should return true if no form is given and all verification forms are complete', () => {
        const isComplete = isVerificationComplete(mockEntityDataAllVerified);
        expect(isComplete).toBe(true);
    });
});
