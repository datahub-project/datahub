import { useApolloClient } from '@apollo/client';
import { Button, Tooltip } from '@components';
import React, { useContext, useEffect, useState } from 'react';
import { useHistory, useParams } from 'react-router';
import { Link } from 'react-router-dom';

import ManageFormContext from '@app/govern/Dashboard/Forms/ManageFormContext';
import { updateFormsList } from '@app/govern/Dashboard/Forms/cacheUtils';
import {
    PUBLISH_EXPLANATION,
    PUBLISH_MODAL_TEXT,
    UNPUBLISH_EXPLANATION,
    UNPUBLISH_MODAL_TEXT,
    mapPromptsToCreatePromptInput,
} from '@app/govern/Dashboard/Forms/formUtils';
import { FooterContainer } from '@app/govern/Dashboard/Forms/styledComponents';
import analytics, { EventType } from '@src/app/analytics';
import { useUserContext } from '@src/app/context/useUserContext';
import { ConfirmationModal } from '@src/app/sharedV2/modals/ConfirmationModal';
import { ToastType, showToastMessage } from '@src/app/sharedV2/toastMessageUtils';
import { useIsThemeV2 } from '@src/app/useIsThemeV2';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { PageRoutes } from '@src/conf/Global';
import { useCreateFormMutation, useUpdateFormMutation } from '@src/graphql/form.generated';
import { FormState, FormType, SearchAcrossEntitiesInput, SearchResults } from '@src/types.generated';

interface Props {
    inputs: SearchAcrossEntitiesInput;
    searchAcrossEntities?: SearchResults | null;
}

const FormFooter = ({ inputs, searchAcrossEntities }: Props) => {
    const isShowNavBarRedesign = useShowNavBarRedesign();
    const history = useHistory();
    const isThemeV2 = useIsThemeV2();
    const me = useUserContext();
    const canEditForms = me.platformPrivileges?.manageDocumentationForms;
    const { form, formValues, setFormValues, setIsFormLoading } = useContext(ManageFormContext);
    const [createForm] = useCreateFormMutation();
    const [updateForm] = useUpdateFormMutation();
    const { urn } = useParams<{ urn: string }>();
    const client = useApolloClient();

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

    const saveForm = (state?: FormState, returnToForms?: boolean) => {
        const showPublishing = state === FormState.Published && returnToForms;
        if (form) {
            form.validateFields().then(() => {
                if (formUrn) {
                    const updateInput = {
                        urn: formUrn,
                        type: formValues.formType,
                        name: formValues.formName,
                        description: formValues.formDescription,
                        prompts: mapPromptsToCreatePromptInput(formValues.questions),
                        actors: {
                            owners: formValues.actors?.owners,
                            users: formValues.actors?.users?.map((user) => user.urn),
                            groups: formValues.actors?.groups?.map((group) => group.urn),
                        },
                        state: state || formValues.state,
                        formAssetAssignment: formValues.assets?.orFilters
                            ? {
                                  orFilters: formValues.assets?.orFilters,
                                  json: formValues.assets?.logicalPredicate
                                      ? JSON.stringify(formValues.assets.logicalPredicate)
                                      : undefined,
                              }
                            : undefined,
                        formSettings: {
                            notificationSettings: {
                                notifyAssigneesOnPublish:
                                    !!formValues.formSettings?.notificationSettings?.notifyAssigneesOnPublish,
                            },
                        },
                    };
                    setIsFormLoading(true);

                    updateForm({
                        variables: {
                            input: updateInput,
                        },
                    })
                        .then((res) => {
                            if (returnToForms) {
                                analytics.event({
                                    type:
                                        state === FormState.Published
                                            ? EventType.PublishFormEvent
                                            : EventType.UnpublishFormEvent,
                                    formUrn,
                                    formType: formValues.formType || FormType.Completion,
                                    noOfQuestions: formValues.questions?.length,
                                    areOwnersAssigned: !!formValues.actors?.owners,
                                    notificationsEnabled:
                                        !!formValues.formSettings?.notificationSettings?.notifyAssigneesOnPublish,
                                });
                            } else if (formValues.state === FormState.Draft) {
                                analytics.event({
                                    type: EventType.SaveFormAsDraftEvent,
                                    formUrn,
                                    formType: formValues.formType || FormType.Completion,
                                    noOfQuestions: formValues.questions?.length,
                                    areOwnersAssigned: !!formValues.actors?.owners,
                                    notificationsEnabled:
                                        !!formValues.formSettings?.notificationSettings?.notifyAssigneesOnPublish,
                                });
                            }
                            showSuccessMessage();
                            updateFormsList(
                                client,
                                inputs,
                                res.data?.updateForm,
                                formUrn,
                                searchAcrossEntities,
                                showPublishing,
                            );

                            if (state) updateFormState(state);
                            if (returnToForms) history.push(`${PageRoutes.GOVERN_DASHBOARD}?documentationTab=forms`);
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
                        prompts: mapPromptsToCreatePromptInput(formValues.questions),
                        actors: {
                            owners: formValues.actors?.owners,
                            users: formValues.actors?.users?.map((user) => user.urn),
                            groups: formValues.actors?.groups?.map((group) => group.urn),
                        },
                        state: state || formValues.state,
                        formAssetAssignment: formValues.assets?.orFilters
                            ? {
                                  orFilters: formValues.assets?.orFilters,
                                  json: formValues.assets?.logicalPredicate
                                      ? JSON.stringify(formValues.assets.logicalPredicate)
                                      : undefined,
                              }
                            : undefined,
                        formSettings: {
                            notificationSettings: {
                                notifyAssigneesOnPublish:
                                    !!formValues.formSettings?.notificationSettings?.notifyAssigneesOnPublish,
                            },
                        },
                    };
                    setIsFormLoading(true);

                    createForm({
                        variables: {
                            input: createInput,
                        },
                    })
                        .then((res) => {
                            analytics.event({
                                type: returnToForms ? EventType.PublishFormEvent : EventType.SaveFormAsDraftEvent,
                                formUrn: res.data?.createForm.urn || '',
                                formType: formValues.formType || FormType.Completion,
                                noOfQuestions: formValues.questions?.length,
                                areOwnersAssigned: !!formValues.actors?.owners,
                                notificationsEnabled:
                                    !!formValues.formSettings?.notificationSettings?.notifyAssigneesOnPublish,
                            });

                            setFormUrn(res.data?.createForm.urn);
                            showSuccessMessage();
                            updateFormsList(
                                client,
                                inputs,
                                res.data?.createForm,
                                res.data?.createForm.urn,
                                searchAcrossEntities,
                                showPublishing,
                            );

                            if (state) updateFormState(state);
                            if (returnToForms) history.push(`${PageRoutes.GOVERN_DASHBOARD}?documentationTab=forms`);
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
        <FooterContainer $showV1Styles={!isThemeV2} $isShowNavBarRedesign={isShowNavBarRedesign}>
            <Link to={`${PageRoutes.GOVERN_DASHBOARD}?documentationTab=forms`}>
                <Button variant="outline" data-testid="form-cancel-button">
                    Cancel
                </Button>
            </Link>
            <Tooltip title="Save the current state of your compliance form." showArrow={false}>
                <Tooltip
                    showArrow={false}
                    title={
                        !canEditForms ? 'Must have permission to manage forms. Ask your DataHub administrator.' : null
                    }
                >
                    <>
                        <Button
                            disabled={!canEditForms}
                            variant="outline"
                            onClick={() => saveForm()}
                            data-testid={FormState.Draft ? 'form-save-draft-button' : 'form-save-button'}
                        >
                            {formValues.state === FormState.Draft ? 'Save Draft' : 'Save'}
                        </Button>
                    </>
                </Tooltip>
            </Tooltip>
            <Tooltip
                title={formValues.state === FormState.Published ? UNPUBLISH_EXPLANATION : PUBLISH_EXPLANATION}
                showArrow={false}
            >
                <Tooltip
                    showArrow={false}
                    title={
                        !canEditForms ? 'Must have permission to manage forms. Ask your DataHub administrator.' : null
                    }
                >
                    <>
                        <Button
                            disabled={!canEditForms}
                            onClick={() => form?.validateFields()?.then(() => setShowConfirmationModal(true))}
                            data-testid={
                                formValues.state === FormState.Published
                                    ? 'form-unpublish-button'
                                    : 'form-publish-button'
                            }
                        >
                            {formValues.state === FormState.Published ? 'Unpublish' : 'Publish'}
                        </Button>
                    </>
                </Tooltip>
            </Tooltip>
            <ConfirmationModal
                isOpen={showConfirmationModal}
                handleClose={handleModalClose}
                handleConfirm={() =>
                    saveForm(
                        formValues.state === FormState.Published ? FormState.Unpublished : FormState.Published,
                        true,
                    )
                }
                modalTitle={`Confirm ${formValues.state === FormState.Published ? 'Unpublish' : 'Publish'}`}
                modalText={formValues.state === FormState.Published ? UNPUBLISH_MODAL_TEXT : PUBLISH_MODAL_TEXT}
                confirmButtonText={formValues.state === FormState.Published ? 'Unpublish' : 'Publish'}
            />
        </FooterContainer>
    );
};

export default FormFooter;
