import { FormType } from '../../../../types.generated';

export type FormMode = 'create' | 'edit';

export type FormQuestion = {
    questionType: string;
    question: string;
    description?: string;
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
