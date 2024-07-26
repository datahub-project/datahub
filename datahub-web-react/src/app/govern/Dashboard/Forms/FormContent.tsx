import React, { useContext } from 'react';
import { FlexBox, FormContentContainer, StepDescription, StepNameHeading } from './styledComponents';
import ManageFormContext from './ManageFormContext';
import { formSteps } from './formUtils';

const FormContent = () => {
    const { currentStep } = useContext(ManageFormContext);

    const activeStep = formSteps.find((step) => step.number === currentStep);

    return (
        <FormContentContainer>
            <FlexBox>
                <StepNameHeading>{activeStep?.name}</StepNameHeading>
                <StepDescription>{activeStep?.description}</StepDescription>
            </FlexBox>
            {activeStep && <activeStep.component />}
        </FormContentContainer>
    );
};

export default FormContent;
