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
    formValues: {},
    setFormValues: () => {},
    form: undefined,
    formMode: 'create',
    setFormMode: () => {},
    isFormLoading: false,
    setIsFormLoading: () => {},
});

export default ManageFormContext;
