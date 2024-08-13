import { Button } from '@src/alchemy-components';
import { showToastMessage, ToastType } from '@src/app/sharedV2/toastMessageUtils';
import { useCreateFormMutation, useUpdateFormMutation } from '@src/graphql/form.generated';
import { CreatePromptInput, FormType } from '@src/types.generated';
import React, { useContext, useEffect, useState } from 'react';
import { useParams } from 'react-router';
import ManageFormContext from './ManageFormContext';
import { FooterContainer } from './styledComponents';

const FormFooter = () => {
    const { form, formValues, setIsFormLoading } = useContext(ManageFormContext);
    const [createForm] = useCreateFormMutation();
    const [updateForm] = useUpdateFormMutation();
    const { urn } = useParams<{ urn: string }>();

    const [formUrn, setFormUrn] = useState<string | undefined>();

    useEffect(() => {
        if (urn) {
            setFormUrn(urn);
        }
    }, [urn]);

    const showErrorMessage = () => {
        showToastMessage(ToastType.ERROR, `Failed to ${formUrn ? 'update' : 'create'} form.`, 3);
    };

    const showSuccessMessage = () => {
        showToastMessage(ToastType.SUCCESS, `Form saved!`, 3);
    };

    const saveForm = () => {
        if (form) {
            form.validateFields().then(() => {
                if (formUrn) {
                    const updateInput = {
                        urn: formUrn,
                        type: formValues.formType,
                        name: formValues.formName,
                        description: formValues.formDescription,
                        prompts: formValues.questions as CreatePromptInput[],
                    };
                    setIsFormLoading(true);

                    updateForm({
                        variables: {
                            input: updateInput,
                        },
                    })
                        .then(() => {
                            showSuccessMessage();
                        })
                        .catch(() => {
                            showErrorMessage();
                        })
                        .finally(() => {
                            setIsFormLoading(false);
                        });
                } else {
                    const createInput = {
                        type: formValues.formType || FormType.Completion,
                        name: formValues.formName || '',
                        description: formValues.formDescription,
                        prompts: formValues.questions as CreatePromptInput[],
                    };
                    setIsFormLoading(true);

                    createForm({
                        variables: {
                            input: createInput,
                        },
                    })
                        .then((res) => {
                            setFormUrn(res.data?.createForm.urn);
                            showSuccessMessage();
                        })
                        .catch(() => {
                            showErrorMessage();
                        })
                        .finally(() => {
                            setIsFormLoading(false);
                        });
                }
            });
        }
    };

    return (
        <FooterContainer>
            <Button variant="outline" onClick={() => saveForm()}>
                Save
            </Button>
            <Button variant="outline">Publish</Button>
        </FooterContainer>
    );
};

export default FormFooter;
