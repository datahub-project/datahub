import React, { useContext, useEffect, useState } from 'react';
import { useParams } from 'react-router';
import { Button, Text } from '@components';
import { ButtonsContainer, FlexBox, FormFooterContainer, NextStepText } from './styledComponents';
import ManageFormContext from './ManageFormContext';
import { FormFields } from './formUtils';
import { useCreateFormMutation, useUpdateFormMutation } from '../../../../graphql/form.generated';
import { showToastMessage, ToastType } from '../../../sharedV2/toastMessageUtils';
import { formSteps } from './formSteps';

const FormFooter = () => {
    const { currentStep, setCurrentStep, form, setIsFormLoading } = useContext(ManageFormContext);
    const [createForm] = useCreateFormMutation();
    const [updateForm] = useUpdateFormMutation();
    const { urn } = useParams<{ urn: string }>();

    const [formUrn, setFormUrn] = useState<string | undefined>();

    const nextStep = formSteps.find((step) => step.number === currentStep + 1);
    const previousStep = formSteps.find((step) => step.number === currentStep - 1);

    useEffect(() => {
        if (urn) {
            setFormUrn(urn);
        }
    }, [urn]);

    const showErrorMessage = () => {
        showToastMessage(ToastType.ERROR, `Failed to ${formUrn ? 'update' : 'create'} form.`, 3);
    };

    const showSuccessMessage = () => {
        showToastMessage(ToastType.SUCCESS, `Saved form.`, 3);
    };

    const saveForm = (formValues: FormFields) => {
        if (form) {
            if (formUrn) {
                const updateInput = {
                    urn: formUrn,
                    type: formValues.formType,
                    name: formValues.formName,
                    description: formValues.formDescription,
                };
                setIsFormLoading(true);

                return updateForm({
                    variables: {
                        input: updateInput,
                    },
                })
                    .then(() => {
                        return true;
                    })
                    .catch(() => {
                        showErrorMessage();
                        return false;
                    })
                    .finally(() => {
                        setIsFormLoading(false);
                    });
            }

            const createInput = {
                type: formValues.formType,
                name: formValues.formName,
                description: formValues.formDescription,
            };
            setIsFormLoading(true);

            return createForm({
                variables: {
                    input: createInput,
                },
            })
                .then((res) => {
                    setFormUrn(res.data?.createForm.urn);
                    return true;
                })
                .catch(() => {
                    showErrorMessage();
                    return false;
                })
                .finally(() => {
                    setIsFormLoading(false);
                });
        }
        return false;
    };

    const navigateToNextStep = () => {
        if (form) {
            form.validateFields()
                .then((formValues) => saveForm(formValues))
                .then((isSuccess) => {
                    if (isSuccess) {
                        setCurrentStep(currentStep + 1);
                        showSuccessMessage();
                    }
                });
        }
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
