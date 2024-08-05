import React from 'react';
import { FormInstance } from 'antd';
import { FormType } from '../../../../types.generated';
import { FormFields, FormMode } from './formUtils';

interface ManageFormState {
    currentStep: number;
    setCurrentStep: React.Dispatch<React.SetStateAction<number>>;
    formValues: FormFields;
    setFormValues: React.Dispatch<React.SetStateAction<FormFields>>;
    form: FormInstance | undefined;
    formMode: FormMode;
    setFormMode: React.Dispatch<React.SetStateAction<FormMode>>;
    isFormLoading: boolean;
    setIsFormLoading: React.Dispatch<React.SetStateAction<boolean>>;
}

const ManageFormContext = React.createContext<ManageFormState>({
    currentStep: 1,
    setCurrentStep: () => {},
    formValues: {
        formType: FormType.Verification,
        formName: '',
    },
    setFormValues: () => {},
    form: undefined,
    formMode: 'create',
    setFormMode: () => {},
    isFormLoading: false,
    setIsFormLoading: () => {},
});

export default ManageFormContext;
