import React, { useContext } from 'react';
import { Button, Text } from '@components';
import { ButtonsContainer, FlexBox, FormFooterContainer, NextStepText } from './styledComponents';
import ManageFormContext from './ManageFormContext';
import { formSteps } from './formUtils';

const FormFooter = () => {
    const { currentStep, setCurrentStep } = useContext(ManageFormContext);
    const nextStep = formSteps.find((step) => step.number === currentStep + 1);
    const previousStep = formSteps.find((step) => step.number === currentStep - 1);

    const navigateToNextStep = () => {
        setCurrentStep(currentStep + 1);
    };

    const navigateToPreviousStep = () => {
        setCurrentStep(currentStep - 1);
    };

    return (
        <FormFooterContainer>
            <FlexBox>
                {nextStep && (
                    <>
                        <NextStepText>Next Step</NextStepText>
                        <Text color="violet" size="md" weight="black">
                            {nextStep?.name}
                        </Text>
                    </>
                )}
            </FlexBox>
            <ButtonsContainer>
                {!!previousStep && (
                    <Button size="lg" variant="outline" onClick={navigateToPreviousStep}>
                        Back
                    </Button>
                )}
                {!!nextStep && (
                    <Button size="lg" onClick={navigateToNextStep}>
                        Next
                    </Button>
                )}
            </ButtonsContainer>
        </FormFooterContainer>
    );
};

export default FormFooter;
