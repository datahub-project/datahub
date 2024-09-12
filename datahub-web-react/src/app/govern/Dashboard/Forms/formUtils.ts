import { LogicalPredicate } from '@src/app/tests/builder/steps/definition/builder/types';
import {
    AndFilterInput,
    CorpGroup,
    CorpUser,
    FormState,
    FormType,
    PromptCardinality,
} from '../../../../types.generated';

export type FormMode = 'create' | 'edit';

export type FormQuestion = {
    id: string;
    type: string;
    title: string;
    description?: string;
    required: boolean;
    structuredPropertyParams?: {
        urn: string;
    };
    ownershipParams?: {
        cardinality: PromptCardinality;
    };
};

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
    questions: FormQuestion[];
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
        label: 'Domain',
        value: 'DOMAIN',
        description: 'This question applies domain to assets',
    },
    {
        label: 'Ownership',
        value: 'OWNERSHIP',
        description: 'This question applies ownership to assets',
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
];
