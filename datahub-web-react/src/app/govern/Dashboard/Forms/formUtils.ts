import { FormType } from '../../../../types.generated';

export type FormMode = 'create' | 'edit';

export type FormQuestion = {
    id?: string;
    type?: string;
    title?: string;
    description?: string;
    required: boolean;
    structuredPropertyParams?: {
        urn: string;
    };
};

export type FormFields = {
    formType?: FormType;
    formName?: string;
    formDescription?: string | undefined;
    questions?: FormQuestion[];
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
        label: 'Structured Properties',
        value: 'STRUCTURED_PROPERTY',
        description: 'This question applies structured properties to assets',
    },
    {
        label: 'Ownership',
        value: 'OWNERSHIP',
        description: 'This question applies ownership to assets',
    },
    {
        label: 'Column-level Structured Properties',
        value: 'FIELDS_STRUCTURED_PROPERTY',
        description: 'This question applies to columns of assets',
    },
];
