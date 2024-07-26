import React from 'react';
import { FormDetails } from './formUtils';
import { FormType } from '../../../../types.generated';

interface Props {
    currentStep: number;
    setCurrentStep: React.Dispatch<React.SetStateAction<number>>;
    details: FormDetails;
    setDetails: React.Dispatch<React.SetStateAction<FormDetails>>;
}

const ManageFormContext = React.createContext<Props>({
    currentStep: 1,
    setCurrentStep: () => {},
    details: {
        formType: FormType.Completion,
        formName: '',
        formDescription: '',
    },
    setDetails: () => {},
});

export default ManageFormContext;
