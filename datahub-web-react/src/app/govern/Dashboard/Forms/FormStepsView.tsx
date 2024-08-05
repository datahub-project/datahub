import React, { useContext } from 'react';
import {
    FormStepsContainer,
    FlexBox,
    StepContainer,
    StepNumber,
    StepName,
    StepsDivider,
    StepIndicator,
} from './styledComponents';
import ManageFormContext from './ManageFormContext';
import ActiveStep from '../../../../images/active-form-step.svg?react';
import InactiveStep from '../../../../images/inactive-form-step.svg?react';
import { formSteps } from './formSteps';

const FormStepsView = () => {
    const { currentStep } = useContext(ManageFormContext);
    return (
        <FormStepsContainer>
            {formSteps.map((formStep) => {
                const isActiveStep = formStep.number === currentStep;
                return (
                    <StepContainer>
                        <StepIndicator>
                            {isActiveStep ? <ActiveStep /> : <InactiveStep />}
                            {formStep.number < formSteps.length && <StepsDivider />}
                        </StepIndicator>
                        <FlexBox>
                            <StepNumber>STEP {formStep.number} </StepNumber>
                            <StepName isActiveStep={isActiveStep}>{formStep.name}</StepName>
                        </FlexBox>
                    </StepContainer>
                );
            })}
        </FormStepsContainer>
    );
};

export default FormStepsView;
