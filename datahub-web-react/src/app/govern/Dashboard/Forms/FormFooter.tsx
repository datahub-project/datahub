import { Button } from '@components';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { showToastMessage, ToastType } from '@src/app/sharedV2/toastMessageUtils';
import { useCreateFormMutation, useUpdateFormMutation } from '@src/graphql/form.generated';
import { CreatePromptInput, FormState, FormType } from '@src/types.generated';
import React, { useContext, useEffect, useState } from 'react';
import { useParams } from 'react-router';
import ManageFormContext from './ManageFormContext';
import { FooterContainer } from './styledComponents';

const FormFooter = () => {
    const { form, formValues, setFormValues, setIsFormLoading } = useContext(ManageFormContext);
    const [createForm] = useCreateFormMutation();
    const [updateForm] = useUpdateFormMutation();
    const { urn } = useParams<{ urn: string }>();

    const [formUrn, setFormUrn] = useState<string | undefined>();

    const [showConfirmationModal, setShowConfirmationModal] = useState<boolean>(false);

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

    const handleModalClose = () => {
        setShowConfirmationModal(false);
    };

    const updateFormState = (state?: FormState) => {
        setFormValues((prev) => ({
            ...prev,
            state,
        }));
    };

    const saveForm = (state?: FormState) => {
        if (form) {
            form.validateFields().then(() => {
                if (formUrn) {
                    const updateInput = {
                        urn: formUrn,
                        type: formValues.formType,
                        name: formValues.formName,
                        description: formValues.formDescription,
                        prompts: formValues.questions as CreatePromptInput[],
                        actors: {
                            owners: formValues.actors?.owners,
                            users: formValues.actors?.users?.map((user) => user.urn),
                            groups: formValues.actors?.groups?.map((group) => group.urn),
                        },
                        state: state || formValues.state,
                    };
                    setIsFormLoading(true);

                    updateForm({
                        variables: {
                            input: updateInput,
                        },
                    })
                        .then(() => {
                            showSuccessMessage();
                            if (state) updateFormState(state);
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
                        actors: {
                            owners: formValues.actors?.owners,
                            users: formValues.actors?.users?.map((user) => user.urn),
                            groups: formValues.actors?.groups?.map((group) => group.urn),
                        },
                        state: state || formValues.state,
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
                            if (state) updateFormState(state);
                        })
                        .catch(() => {
                            showErrorMessage();
                        })
                        .finally(() => {
                            setIsFormLoading(false);
                        });
                }
            });
            setShowConfirmationModal(false);
        }
    };

    return (
        <FooterContainer>
            <Button variant="outline" onClick={() => saveForm()}>
                Save
            </Button>
            <Button variant="outline" onClick={() => form?.validateFields().then(() => setShowConfirmationModal(true))}>
                {formValues.state === FormState.Published ? 'Unpublish' : 'Publish'}
            </Button>
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={handleModalClose}
                handleConfirm={() =>
                    saveForm(formValues.state === FormState.Published ? FormState.Unpublished : FormState.Published)
                }
                modalTitle={`Confirm ${formValues.state === FormState.Published ? 'Unpublish' : 'Publish'}`}
                modalText={`Are you sure you want to ${
                    formValues.state === FormState.Published ? 'unpublish' : 'publish'
                } the form?`}
                confirmButtonText={formValues.state === FormState.Published ? 'Unpublish' : 'Publish'}
            />
        </FooterContainer>
    );
};

export default FormFooter;
