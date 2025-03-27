import { useState } from 'react';
import { useEntityData } from '@src/app/entity/shared/EntityContext';
import { useRaiseIncidentMutation, useUpdateIncidentMutation } from '@src/graphql/mutations.generated';
import { EntityType, IncidentSourceType, IncidentState } from '@src/types.generated';
import analytics, { EntityActionType, EventType } from '@src/app/analytics';
import _ from 'lodash';
import { Form, message } from 'antd';
import { useApolloClient } from '@apollo/client';
import handleGraphQLError from '@src/app/shared/handleGraphQLError';
import { IncidentAction } from '../../constant';
import { PAGE_SIZE, updateActiveIncidentInCache } from '../../incidentUtils';

export const getCacheIncident = ({
    values,
    responseData,
    user,
    incidentUrn,
}: {
    values: any;
    responseData?: any;
    user?: any;
    incidentUrn?: string;
}) => {
    const newIncident = {
        urn: incidentUrn ?? responseData?.data?.raiseIncident,
        type: EntityType.Incident,
        incidentType: values.type,
        customType: values.customType || null,
        title: values.title,
        description: values.description,
        startedAt: null,
        tags: null,
        status: {
            state: values?.state,
            stage: values?.stage,
            message: values?.message || null,
            lastUpdated: {
                __typename: 'AuditStamp',
                time: Date.now(),
                actor: user?.urn,
            },
        },
        source: {
            type: IncidentSourceType.Manual,
            source: {
                urn: '',
                type: 'Assertion',
                platform: {
                    urn: '',
                    name: '',
                    properties: { displayName: '', logoUrl: '' },
                },
            },
        },
        linkedAssets: {
            relationships: values.linkedAssets?.map((linkedAsset) => ({
                entity: {
                    ...linkedAsset,
                },
            })),
        },

        priority: values.priority,
        created: {
            time: Date.now(),
            actor: user?.urn,
        },
        assignees: values.assignees,
    };
    return newIncident;
};

export const useIncidentHandler = ({ mode, onSubmit, incidentUrn, onClose, user, assignees, linkedAssets, entity }) => {
    const [raiseIncidentMutation] = useRaiseIncidentMutation();
    const [updateIncidentMutation] = useUpdateIncidentMutation();
    const [form] = Form.useForm();
    const { urn, entityType } = useEntityData();
    const client = useApolloClient();
    const isAddIncidentMode = mode === IncidentAction.CREATE;

    const handleAddIncident = async (input: any) => {
        return raiseIncidentMutation({
            variables: {
                input: {
                    ...input,
                    priority: input.priority || undefined,
                    status: {
                        ...input.status,
                        stage: input.status.stage || undefined,
                    },
                },
            },
        });
    };

    const handleUpdateIncident = async (input: any, incidentUpdateUrn: string) => {
        return updateIncidentMutation({
            variables: {
                input: {
                    ...input,
                    priority: input.priority || null,
                    status: {
                        ...input.status,
                        stage: input.status.stage || null,
                    },
                },
                urn: incidentUpdateUrn,
            },
        });
    };

    const showMessage = (content: string) => {
        message.success({
            content,
            duration: 2,
        });
    };

    const finalizeSubmission = () => {
        onSubmit?.();
        onClose?.();
    };

    const handleSubmissionError = (error: any) => {
        const action = isAddIncidentMode ? 'raise' : 'update';
        handleGraphQLError({
            error,
            defaultMessage: `Failed to ${action} incident!`,
            permissionMessage: `Unauthorized to ${action} incident.`,
        });
    };

    const [isLoading, setIsLoading] = useState(false);
    const handleSubmit = async () => {
        try {
            setIsLoading(true);

            const values = form.getFieldsValue();
            const baseInput = {
                ...values,
                resourceUrn: entity?.urn || urn,
                status: {
                    stage: values.status,
                    state: values.state || IncidentState.Active,
                    message: values.message,
                },
            };
            const newInput = _.omit(baseInput, ['state', 'message']);
            const newUpdateInput = _.omit(newInput, ['resourceUrn', 'type', 'customType']);
            const input = !isAddIncidentMode ? newUpdateInput : newInput;

            if (isAddIncidentMode) {
                const responseData: any = await handleAddIncident(input);
                if (responseData) {
                    showMessage('Incident Added');
                }
                const newIncident = getCacheIncident({
                    values: {
                        ...values,
                        state: baseInput.status.state,
                        stage: baseInput.status.stage,
                        message: baseInput.status.message,
                        assignees,
                        linkedAssets,
                    },
                    incidentUrn: responseData?.data?.raiseIncident,
                    user,
                });
                updateActiveIncidentInCache(client, urn, newIncident, PAGE_SIZE);
                analytics.event({
                    type: EventType.EntityActionEvent,
                    entityType,
                    entityUrn: urn,
                    actionType: EntityActionType.AddIncident,
                });
            } else if (incidentUrn) {
                const updatedIncidentResponse: any = await handleUpdateIncident(input, incidentUrn);
                if (updatedIncidentResponse?.data?.updateIncident) {
                    const updatedIncident = getCacheIncident({
                        values: {
                            ...values,
                            state: baseInput.status.state,
                            stage: baseInput.status.stage || '',
                            message: baseInput.status.message,
                            priority: values.priority || null,
                            assignees,
                            linkedAssets,
                        },
                        user,
                        incidentUrn,
                    });
                    updateActiveIncidentInCache(client, urn, updatedIncident, PAGE_SIZE);
                }
                showMessage('Incident Updated');
            }

            finalizeSubmission();
        } catch (error: any) {
            console.log('error>>>>>', error);
            handleSubmissionError(error);
        } finally {
            setIsLoading(false); // Stop loading
        }
    };
    return { handleSubmit, form, isLoading };
};
