import { FormState } from '@src/types.generated';
import { FormInstance } from 'antd';
import React from 'react';
import { FormFields, FormMode } from './formUtils';

interface ManageFormState {
    formValues: FormFields;
    setFormValues: React.Dispatch<React.SetStateAction<FormFields>>;
    form: FormInstance | undefined;
    formMode: FormMode;
    setFormMode: React.Dispatch<React.SetStateAction<FormMode>>;
    isFormLoading: boolean;
    setIsFormLoading: React.Dispatch<React.SetStateAction<boolean>>;
}

const ManageFormContext = React.createContext<ManageFormState>({
    formValues: {
        formName: undefined,
        formType: undefined,
        questions: [],
        actors: {
            owners: false,
            users: [],
            groups: [],
        },
        state: FormState.Draft,
    },
    setFormValues: () => {},
    form: undefined,
    formMode: 'create',
    setFormMode: () => {},
    isFormLoading: false,
    setIsFormLoading: () => {},
});

export default ManageFormContext;
