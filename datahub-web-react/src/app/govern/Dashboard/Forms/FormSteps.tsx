import React from 'react';
import { Text } from '@components';
import { formSteps } from './formUtils';
import { FormStepsContainer, FlexBox } from './styledComponents';

const FormSteps = () => {
    return (
        <FormStepsContainer>
            {formSteps.map((formStep) => {
                return (
                    <FlexBox>
                        <Text color="gray" size="md">
                            STEP {formStep.number}
                        </Text>
                        <Text color="violet" size="xl" weight="bold">
                            {formStep.name}
                        </Text>
                    </FlexBox>
                );
            })}
        </FormStepsContainer>
    );
};

export default FormSteps;
