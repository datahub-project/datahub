import React, { useState } from 'react';
import ManageFormContext from './ManageFormContext';
import { FormDetails } from './formUtils';
import { FormType } from '../../../../types.generated';

export const ManageFormContextProvider = ({ children }: { children: React.ReactNode }) => {
    const formDetails: FormDetails = {
        formType: FormType.Completion,
        formName: '',
        formDescription: '',
    };
    const [currentStep, setCurrentStep] = useState(1);
    const [details, setDetails] = useState(formDetails);

    return (
        <ManageFormContext.Provider
            value={{
                currentStep,
                setCurrentStep,
                details,
                setDetails,
            }}
        >
            {children}
        </ManageFormContext.Provider>
    );
};
