import { Maybe } from 'graphql/jsutils/Maybe';
import {
    FieldFormPromptAssociation,
    FormAssociation,
    FormPrompt,
    FormPromptAssociation,
    FormType,
    ResolvedAuditStamp,
    SchemaField,
} from '../../../../../../../types.generated';
import { SCHEMA_FIELD_PROMPT_TYPES } from '../../../../entityForm/constants';
import { GenericEntityProperties } from '../../../../types';

export function getFormAssociations(entityData: GenericEntityProperties | null) {
    return [...(entityData?.forms?.incompleteForms || []), ...(entityData?.forms?.completedForms || [])];
}

export function getFormAssociation(formUrn: string, entityData: GenericEntityProperties | null) {
    return (
        entityData?.forms?.incompleteForms?.find((formAssoc) => formAssoc.form.urn === formUrn) ||
        entityData?.forms?.completedForms?.find((formAssoc) => formAssoc.form.urn === formUrn)
    );
}

/*
 * For a given prompt, get all of the completed field prompts.
 * Takes in an optional set of prompt IDs, if this exists, only return the
 * completed field prompts for this prompt if this prompt ID is in the set.
 */
function getCompletedFieldPromptsFromPrompt(prompt: FormPromptAssociation, relevantFieldFormPromptIds?: Set<string>) {
    if (relevantFieldFormPromptIds && relevantFieldFormPromptIds.has(prompt.id)) {
        return prompt.fieldAssociations?.completedFieldPrompts || [];
    }
    if (!relevantFieldFormPromptIds) {
        return prompt.fieldAssociations?.completedFieldPrompts || [];
    }
    return [];
}

/*
 * For a given form, loop over all prompts and retrieve completedFieldPrompts from each.
 * Note: we pass in an optional set of prompt IDs to choose from in order to get completed
 * field prompts for a certain set of entity prompts id we choose.
 */
export function getCompletedFieldPromptsFromForm(
    formAssociation: FormAssociation,
    relevantFieldFormPromptIds?: Set<string>,
) {
    let completedFieldPromptAssociations: FieldFormPromptAssociation[] = [];
    formAssociation.completedPrompts?.forEach((completedPrompt) => {
        completedFieldPromptAssociations = completedFieldPromptAssociations.concat(
            getCompletedFieldPromptsFromPrompt(completedPrompt, relevantFieldFormPromptIds),
        );
    });
    formAssociation.incompletePrompts?.forEach((incompletPrompt) => {
        completedFieldPromptAssociations = completedFieldPromptAssociations.concat(
            getCompletedFieldPromptsFromPrompt(incompletPrompt, relevantFieldFormPromptIds),
        );
    });
    return completedFieldPromptAssociations;
}

/*
 * Gets a list of the completed field prompt associations which live as children to top
 * level prompt associations for each schema field.
 * We need to loop over every prompt both completed and incomplete, form complete and incomplete forms.
 * For each prompt, get their list of completedFieldPrompts
 * Takes in an optional list of relevant prompt IDs to filter results down.
 */
export function getCompletedFieldPromptAssociations(
    entityData: GenericEntityProperties | null,
    relevantFieldFormPromptIds?: Set<string>,
) {
    let completedFieldPromptAssociations: FieldFormPromptAssociation[] = [];

    const forms = entityData?.forms;
    forms?.completedForms?.forEach((formAssociation) => {
        completedFieldPromptAssociations = completedFieldPromptAssociations.concat(
            getCompletedFieldPromptsFromForm(formAssociation, relevantFieldFormPromptIds),
        );
    });
    forms?.incompleteForms?.forEach((formAssociation) => {
        completedFieldPromptAssociations = completedFieldPromptAssociations.concat(
            getCompletedFieldPromptsFromForm(formAssociation, relevantFieldFormPromptIds),
        );
    });

    return completedFieldPromptAssociations;
}

/*
 * For a given form, gets a list of the completed field prompt associations which live
 * as children to top level prompt associations for each schema field.
 */
export function getCompletedFieldPromptAssociationsForForm(
    formUrn: string,
    entityData: GenericEntityProperties | null,
    relevantFieldFormPromptIds?: Set<string>,
) {
    const formAssociation = getFormAssociation(formUrn, entityData);
    return formAssociation ? getCompletedFieldPromptsFromForm(formAssociation, relevantFieldFormPromptIds) : [];
}

export function getNumPromptsCompletedForField(
    fieldPath: string,
    entityData: GenericEntityProperties | null,
    formUrn: string,
) {
    const completedFieldPromptAssociations = getCompletedFieldPromptAssociationsForForm(formUrn, entityData);
    return completedFieldPromptAssociations.filter((association) => association.fieldPath === fieldPath).length;
}

/*
 * Returns the number of schema field prompts not completed yet.
 * The total number of schema field prompts equals the top level number of field prompts
 * on the form multiplied by the number of schema fields.
 * Optionally takes in a formUrn to look at one specific form or all forms
 */
export function getNumSchemaFieldPromptsRemaining(
    entityData: GenericEntityProperties | null,
    fieldFormPrompts: FormPrompt[],
    numSchemaFields: number,
    formUrn?: string,
) {
    const numFieldPromptsAvailable = numSchemaFields * fieldFormPrompts.length;
    // we pass in either required or optional fieldFormPrompts that we care about in this method, need to check against these
    const relevantFieldFormPromptIds = new Set(fieldFormPrompts.map((prompt) => prompt.id));
    const completedFieldPromptAssociations = formUrn
        ? getCompletedFieldPromptAssociationsForForm(formUrn, entityData, relevantFieldFormPromptIds)
        : getCompletedFieldPromptAssociations(entityData, relevantFieldFormPromptIds);

    return numFieldPromptsAvailable - completedFieldPromptAssociations.length;
}

// Get completed prompts from both complete and incomplete forms for this entity
export function getCompletedPrompts(entityData: GenericEntityProperties | null) {
    const forms = entityData?.forms;
    let completedPrompts =
        forms?.incompleteForms?.flatMap((form) => (form.completedPrompts ? form.completedPrompts : [])) || [];
    completedPrompts = completedPrompts.concat(
        forms?.completedForms?.flatMap((form) => (form.completedPrompts ? form.completedPrompts : [])) || [],
    );
    return completedPrompts;
}

// Get incomplete prompts from both complete and incomplete forms for this entity
export function getIncompletePrompts(entityData: GenericEntityProperties | null) {
    const forms = entityData?.forms;
    let incompletePrompts =
        forms?.incompleteForms?.flatMap((form) => (form.incompletePrompts ? form.incompletePrompts : [])) || [];
    incompletePrompts = incompletePrompts.concat(
        forms?.completedForms?.flatMap((form) => (form.incompletePrompts ? form.incompletePrompts : [])) || [],
    );
    return incompletePrompts;
}

export function isPromptComplete(prompt: FormPrompt, completedPrompts: FormPromptAssociation[]) {
    return !!completedPrompts.find((completedPrompt) => completedPrompt.id === prompt.id);
}

export function findCompletedFieldPrompt(fieldPrompt?: SchemaField, promptAssociation?: FormPromptAssociation) {
    return promptAssociation?.fieldAssociations?.completedFieldPrompts?.find(
        (fieldPath) => fieldPath.fieldPath === fieldPrompt?.fieldPath,
    );
}

export function isFieldPromptComplete(fieldPrompt, promptAssociation) {
    return !!findCompletedFieldPrompt(fieldPrompt, promptAssociation);
}

// For every prompt provided, check if it's in our list of completed prompts and return number prompts not completed
export function getNumEntityPromptsRemaining(entityPrompts: FormPrompt[], entityData: GenericEntityProperties | null) {
    const completedPrompts = getCompletedPrompts(entityData);
    let numPromptsRemaining = 0;

    entityPrompts.forEach((prompt) => {
        if (prompt && !isPromptComplete(prompt, completedPrompts)) {
            numPromptsRemaining += 1;
        }
    });

    return numPromptsRemaining;
}

// Get prompts from both complete and incomplete forms
export function getAllPrompts(entityData: GenericEntityProperties | null) {
    let prompts = entityData?.forms?.incompleteForms?.flatMap((form) => form.form.info.prompts) || [];
    prompts = prompts.concat(entityData?.forms?.completedForms?.flatMap((form) => form.form.info.prompts) || []);
    return prompts;
}

// Find a specific prompt association from both complete and incomplete prompts
export function findPromptAssociation(prompt: FormPrompt, allPrompts: Array<FormPromptAssociation>) {
    return allPrompts?.find((myprompt) => myprompt.id === prompt.id);
}

// Get the prompts for a given form
export function getPromptsForForm(formUrn: string, entityData: GenericEntityProperties | null) {
    const formAssociation = getFormAssociation(formUrn, entityData);
    return formAssociation?.form?.info?.prompts || [];
}

/*
 * Gets information for entity level prompts
 */
export function getEntityPromptsInfo(prompts: FormPrompt[], entityData: GenericEntityProperties | null) {
    const entityPrompts = prompts.filter((prompt) => !SCHEMA_FIELD_PROMPT_TYPES.includes(prompt.type));
    const requiredEntityPrompts = entityPrompts.filter((prompt) => prompt.required);
    const optionalEntityPrompts = entityPrompts.filter((prompt) => !prompt.required);

    const numRequiredEntityPromptsRemaining = getNumEntityPromptsRemaining(requiredEntityPrompts, entityData);
    const numOptionalEntityPromptsRemaining = getNumEntityPromptsRemaining(optionalEntityPrompts, entityData);

    return {
        entityPrompts,
        numRequiredEntityPromptsRemaining,
        numOptionalEntityPromptsRemaining,
        requiredEntityPrompts,
    };
}

/*
 * Gets information for schema field level prompts
 */
export function getFieldPromptsInfo(
    prompts: FormPrompt[],
    entityData: GenericEntityProperties | null,
    numSchemaFields: number,
    formUrn?: string,
) {
    const fieldPrompts = prompts.filter((prompt) => SCHEMA_FIELD_PROMPT_TYPES.includes(prompt.type));
    const requiredFieldPrompts = fieldPrompts.filter((prompt) => prompt.required);
    const optionalFieldPrompts = fieldPrompts.filter((prompt) => !prompt.required);

    const numRequiredFieldPromptsRemaining = getNumSchemaFieldPromptsRemaining(
        entityData,
        requiredFieldPrompts,
        numSchemaFields,
        formUrn,
    );
    const numOptionalFieldPromptsRemaining = getNumSchemaFieldPromptsRemaining(
        entityData,
        optionalFieldPrompts,
        numSchemaFields,
        formUrn,
    );

    return { fieldPrompts, numRequiredFieldPromptsRemaining, numOptionalFieldPromptsRemaining, requiredFieldPrompts };
}

export function getFormVerification(formUrn: string, entityData: GenericEntityProperties | null) {
    return entityData?.forms?.verifications?.find((verification) => verification.form.urn === formUrn);
}

export function getVerificationForms(entityData: GenericEntityProperties | null) {
    const formAssociations = getFormAssociations(entityData);
    return formAssociations.filter((formAssociation) => formAssociation.form.info.type === FormType.Verification);
}

export function areAllFormsVerified(formAssociations: FormAssociation[], entityData: GenericEntityProperties | null) {
    return formAssociations.every((formAssociation) => !!getFormVerification(formAssociation.form.urn, entityData));
}

/*
 * If a form urn is supplied, return true if that form is verified.
 * If no form is supplied, return true if all verification type forms are verified.
 * If there are no verification type forms or any are missing verification, return false.
 */
export function isVerificationComplete(entityData: GenericEntityProperties | null, formUrn?: string) {
    if (formUrn) {
        return !!getFormVerification(formUrn, entityData);
    }
    const verificationForms = getVerificationForms(entityData);
    if (verificationForms.length) {
        return areAllFormsVerified(verificationForms, entityData);
    }
    return false;
}

export function isFormVerificationType(entityData: GenericEntityProperties | null, formUrn: string) {
    const formAssociation = getFormAssociation(formUrn, entityData);
    return formAssociation?.form?.info?.type === FormType.Verification;
}

/*
 * If given a single form we should show verification copy and styles if it is of type verification.
 * If no formUrn is supplied, return true if any of our multiple forms are of type verification.
 */
export function shouldShowVerificationInfo(entityData: GenericEntityProperties | null, formUrn?: string) {
    if (formUrn) {
        return isFormVerificationType(entityData, formUrn);
    }
    return getVerificationForms(entityData).length > 0;
}

function getMostRecentVerificationAuditStamp(entityData: GenericEntityProperties | null) {
    let mostRecentTimestamp: Maybe<ResolvedAuditStamp> = null;
    entityData?.forms?.verifications?.forEach((verification) => {
        if (mostRecentTimestamp === null || (verification.lastModified?.time || 0) > (mostRecentTimestamp?.time || 0)) {
            mostRecentTimestamp = verification.lastModified;
        }
    });
    return mostRecentTimestamp;
}

/*
 * If given one form, return the verification lastModified for it. Otherwise, find the most
 * recently completed verification time stamp from any of the forms on this entity
 */
export function getVerificationAuditStamp(entityData: GenericEntityProperties | null, formUrn?: string) {
    if (formUrn) {
        return getFormVerification(formUrn, entityData)?.lastModified || null;
    }
    return getMostRecentVerificationAuditStamp(entityData);
}

export function getBulkByQuestionPrompts(formUrn: string, entityData: GenericEntityProperties | null) {
    const formAssociation = getFormAssociation(formUrn, entityData);
    return (
        formAssociation?.form?.info?.prompts?.filter((prompt) => !SCHEMA_FIELD_PROMPT_TYPES.includes(prompt.type)) || []
    );
}
