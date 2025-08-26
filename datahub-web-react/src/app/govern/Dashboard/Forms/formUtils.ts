import { ColorOptions } from '@components/theme/config';

import { LogicalPredicate } from '@src/app/tests/builder/steps/definition/builder/types';

import {
    AndFilterInput,
    AssignmentStatus,
    CorpGroup,
    CorpUser,
    CreatePromptInput,
    DomainParams,
    DomainParamsInput,
    FormAssignmentStatus,
    FormPrompt,
    FormState,
    FormType,
    GlossaryTermsParams,
    GlossaryTermsParamsInput,
    OwnershipParams,
    OwnershipParamsInput,
    StructuredPropertyParams,
    StructuredPropertyParamsInput,
} from '@types';

export type FormMode = 'create' | 'edit';

export type FormActors = {
    owners?: boolean;
    users?: CorpUser[];
    groups?: CorpGroup[];
};

export type FormAssets = {
    orFilters?: AndFilterInput[];
    logicalPredicate?: LogicalPredicate;
};

export type FormSettings = {
    notificationSettings?: FormNotificationSettings;
};

export type FormNotificationSettings = {
    notifyAssigneesOnPublish?: boolean;
};

export type FormFields = {
    formType?: FormType;
    formName?: string;
    formDescription?: string | undefined;
    questions: FormPrompt[];
    actors?: FormActors;
    state?: FormState;
    assets?: FormAssets;
    formSettings?: FormSettings;
};

export const handleInputChange = (setFormValues) => (event) => {
    const { id, value } = event.target;
    setFormValues((prev) => ({
        ...prev,
        [id]: value,
    }));
};

export const questionTypes = [
    {
        label: 'Ownership',
        value: 'OWNERSHIP',
        description: 'Require ownership for all assets assigned to this form',
        defaultTitle: 'Select owners for this asset',
        defaultDescription: 'Choose the owners of this asset.',
    },
    {
        label: 'Documentation',
        value: 'DOCUMENTATION',
        description: 'Require documentation for all assets assigned to this form',
        defaultTitle: 'Provide a description for this asset',
        defaultDescription: 'Provide a detailed description for this data asset.',
    },
    {
        label: 'Column Documentation',
        value: 'FIELDS_DOCUMENTATION',
        description: 'Require documentation for all columns of the assets assigned to this form',
        defaultTitle: 'Provide a description for this column',
        defaultDescription: 'Provide a detailed description for this column.',
    },
    {
        label: 'Glossary Terms',
        value: 'GLOSSARY_TERMS',
        description: 'Require glossary terms for all assets assigned to this form',
        defaultTitle: 'Select Glossary Terms for this asset',
        defaultDescription: 'Choose Glossary Terms that should be applied to this asset.',
    },
    {
        label: 'Column Glossary Terms',
        value: 'FIELDS_GLOSSARY_TERMS',
        description: 'Require glossary terms for all columns of the assets assigned to this form',
        defaultTitle: 'Select Glossary Terms for this column',
        defaultDescription: 'Choose Glossary Terms that should be applied to this column.',
    },
    {
        label: 'Structured Property',
        value: 'STRUCTURED_PROPERTY',
        description: 'Require a specific type of structured property for all assets assigned to this form',
        defaultTitle: '',
        defaultDescription: '',
    },

    {
        label: 'Column Structured Property',
        value: 'FIELDS_STRUCTURED_PROPERTY',
        description:
            'Require a specific type of structured property for all the columns of assets assigned to this form',
        defaultTitle: '',
        defaultDescription: '',
    },
    {
        label: 'Domain',
        value: 'DOMAIN',
        description: 'Require a domain for all assets assigned to this form',
        defaultTitle: 'Select a Domain for this asset',
        defaultDescription: 'Select the Domain that is responsible for this asset',
    },
];

function mapStructuredPropertyParams(params: StructuredPropertyParams): StructuredPropertyParamsInput {
    return { urn: params.structuredProperty.urn };
}

function mapGlossaryTermsParams(params: GlossaryTermsParams): GlossaryTermsParamsInput {
    const allowedTerms = params.allowedTerms ? params.allowedTerms : params.resolvedAllowedTerms;
    return { cardinality: params.cardinality, allowedTerms: allowedTerms?.map((t) => t.urn) };
}

function mapOwnershipParams(params: OwnershipParams): OwnershipParamsInput {
    return {
        cardinality: params.cardinality,
        allowedOwners: params.allowedOwners?.map((o) => o.urn),
        allowedOwnershipTypes: params.allowedOwnershipTypes?.map((o) => o.urn),
    };
}

function mapDomainParams(params: DomainParams): DomainParamsInput {
    return { allowedDomains: params.allowedDomains?.map((o) => o.urn) };
}

export function mapPromptsToCreatePromptInput(prompts: FormPrompt[]): CreatePromptInput[] {
    return prompts.map((prompt) => ({
        id: prompt.id,
        description: prompt.description,
        type: prompt.type,
        title: prompt.title,
        required: prompt.required,
        structuredPropertyParams: prompt.structuredPropertyParams
            ? mapStructuredPropertyParams(prompt.structuredPropertyParams)
            : undefined,
        ownershipParams: prompt.ownershipParams ? mapOwnershipParams(prompt.ownershipParams) : undefined,
        glossaryTermsParams: prompt.glossaryTermsParams
            ? mapGlossaryTermsParams(prompt.glossaryTermsParams)
            : undefined,
        domainParams: prompt.domainParams ? mapDomainParams(prompt.domainParams) : undefined,
    }));
}

export const PUBLISH_EXPLANATION = 'Publishing will assign the form to the selected assets and users.';
export const PUBLISH_MODAL_TEXT = `Are you sure? ${PUBLISH_EXPLANATION}`;
export const UNPUBLISH_EXPLANATION = 'Unpublishing will hide this form from selected assets and users.';
export const UNPUBLISH_MODAL_TEXT = `Are you sure? ${UNPUBLISH_EXPLANATION}`;

export function isFormAssignmentInProgress(formStatus: FormState, assignmentStatus?: FormAssignmentStatus | null) {
    const now = new Date().getTime();
    const oneDayAgo = new Date(now - 1 * 24 * 60 * 60 * 1000).getTime();

    return (
        assignmentStatus?.status === AssignmentStatus.InProgress &&
        formStatus === FormState.Published &&
        (assignmentStatus.timestamp || 0) > oneDayAgo
    );
}

export function getStatusDetails(formStatus: FormState, assignmentStatus?: FormAssignmentStatus | null) {
    const isInProgress = isFormAssignmentInProgress(formStatus, assignmentStatus);
    let colorScheme: ColorOptions = 'gray';
    if (isInProgress) colorScheme = 'blue';
    else if (formStatus === FormState.Published) colorScheme = 'primary';
    else if (formStatus === FormState.Unpublished) colorScheme = 'red';
    const label = isInProgress ? 'Publishing' : formStatus?.charAt(0) + formStatus?.slice(1)?.toLowerCase();

    return { label, colorScheme };
}
