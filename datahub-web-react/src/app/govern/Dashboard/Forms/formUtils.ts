import { LogicalPredicate } from '@src/app/tests/builder/steps/definition/builder/types';
import {
    AndFilterInput,
    CorpGroup,
    CorpUser,
    CreatePromptInput,
    DomainParams,
    DomainParamsInput,
    FormState,
    FormType,
    FormPrompt,
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
        description: 'This question applies ownership to assets',
    },
    {
        label: 'Documentation',
        value: 'DOCUMENTATION',
        description: 'This question applies documentation to assets',
    },
    {
        label: 'Documentation - Column',
        value: 'FIELDS_DOCUMENTATION',
        description: 'This question applies documentation to columns of assets',
    },
    {
        label: 'Glossary Terms',
        value: 'GLOSSARY_TERMS',
        description: 'This question applies glossary terms to assets',
    },
    {
        label: 'Glossary Terms - Column',
        value: 'FIELDS_GLOSSARY_TERMS',
        description: 'This question applies glossary terms to columns of assets',
    },
    {
        label: 'Structured Property',
        value: 'STRUCTURED_PROPERTY',
        description: 'This question applies structured property to assets',
    },

    {
        label: 'Structured Property - Column',
        value: 'FIELDS_STRUCTURED_PROPERTY',
        description: 'This question applies structured property to columns of assets',
    },
    {
        label: 'Domain',
        value: 'DOMAIN',
        description: 'This question applies domain to assets',
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
