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
        __typename: 'Incident',
        urn: incidentUrn ?? responseData?.data?.raiseIncident,
        type: EntityType.Incident,
        incidentType: values.type,
        customType: values.customType || null,
        title: values.title,
        description: values.description,
        startedAt: null,
        tags: null,
        status: {
            __typename: 'IncidentStatus',
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
            __typename: 'IncidentSource',
            type: IncidentSourceType.Manual,
            source: null,
        },
        linkedAssets: {
            __typename: 'EntityRelationshipsResult',
            relationships: values.linkedAssets?.map((linkedAsset) => ({
                entity: {
                    ...linkedAsset,
                },
            })),
        },

        priority: values.priority,
        created: {
            __typename: 'AuditStamp',
            time: values.created || new Date(),
            actor: user?.urn,
        },
        assignees: values.assignees,
    };
    return newIncident;
};

export const useIncidentHandler = ({ mode, onSubmit, incidentUrn, user, assignees, linkedAssets, entity }) => {
    // Important: Here we are trying to fetch the URN of the sibling whose "profile" we are currently viewing.
    // We then insert any new incidents into this cache as well so that it immediately updates the page for the asset.
    const { urn: maybeCacheEntityUrn } = useEntityData();
    const [raiseIncidentMutation] = useRaiseIncidentMutation();
    const [updateIncidentMutation] = useUpdateIncidentMutation();
    const [form] = Form.useForm();
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
                status: {
                    stage: values.status,
                    state: values.state || IncidentState.Active,
                    message: values.message,
                },
            };
            const newInput = _.omit(baseInput, ['state', 'message']);
            const newUpdateInput = _.omit(newInput, ['resourceUrn', 'type', 'resourceUrns', 'customType']);
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
                // Add new incident to core entity's cache.
                updateActiveIncidentInCache(client, entity.urn, newIncident, PAGE_SIZE);

                if (maybeCacheEntityUrn) {
                    // Optional: Also add into the cache of the sibling whose page we are viewing.
                    updateActiveIncidentInCache(client, maybeCacheEntityUrn, newIncident, PAGE_SIZE);
                }
                analytics.event({
                    type: EventType.EntityActionEvent,
                    entityType: entity?.entityType,
                    entityUrn: entity.urn,
                    actionType: EntityActionType.AddIncident,
                });
            } else if (incidentUrn) {
                await handleUpdateIncident(input, incidentUrn);
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
