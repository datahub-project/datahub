import { LogicalPredicate } from '@src/app/tests/builder/steps/definition/builder/types';
import {
    AndFilterInput,
    CorpGroup,
    CorpUser,
    CreatePromptInput,
    DomainParams,
    DomainParamsInput,
    FormPrompt,
    FormState,
    FormType,
    GlossaryTermsParams,
    GlossaryTermsParamsInput,
    OwnershipParams,
    OwnershipParamsInput,
    StructuredPropertyParams,
    StructuredPropertyParamsInput,
} from '../../../../types.generated';

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

export type FormFields = {
    formType?: FormType;
    formName?: string;
    formDescription?: string | undefined;
    questions: FormPrompt[];
    actors?: FormActors;
    state?: FormState;
    assets?: FormAssets;
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
    },
    {
        label: 'Documentation',
        value: 'DOCUMENTATION',
        description: 'Require documentation for all assets assigned to this form',
    },
    {
        label: 'Column Documentation',
        value: 'FIELDS_DOCUMENTATION',
        description: 'Require documentation for all columns of the assets assigned to this form',
    },
    {
        label: 'Glossary Terms',
        value: 'GLOSSARY_TERMS',
        description: 'Require glossary terms for all assets assigned to this form',
    },
    {
        label: 'Column Glossary Terms',
        value: 'FIELDS_GLOSSARY_TERMS',
        description: 'Require glossary terms for all columns of the assets assigned to this form',
    },
    {
        label: 'Structured Property',
        value: 'STRUCTURED_PROPERTY',
        description: 'Require a specific type of structured property for all assets assigned to this form',
    },

    {
        label: 'Column Structured Property',
        value: 'FIELDS_STRUCTURED_PROPERTY',
        description:
            'Require a specific type of structured property for all the columns of assets assigned to this form',
    },
    {
        label: 'Domain',
        value: 'DOMAIN',
        description: 'Require a domain for all assets assigned to this form',
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
