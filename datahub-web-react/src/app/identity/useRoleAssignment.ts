import { ApolloClient } from '@apollo/client';
import i18next from 'i18next';
import { useCallback, useState } from 'react';

import { clearRoleListCache } from '@app/permissions/roles/cacheUtils';
import { toast } from '@src/alchemy-components';

import { useBatchAssignRoleMutation } from '@graphql/mutations.generated';
import { DataHubRole } from '@types';

export const NO_ROLE_URN = 'urn:li:dataHubRole:NoRole';

interface RoleAssignmentState {
    isViewingAssignRole: boolean;
    actorUrn: string;
    actorName: string;
    currentRoleUrn: string;
    originalRoleUrn: string;
}

interface UseRoleAssignmentOptions {
    entityLabel: string;
    selectRoleOptions: DataHubRole[];
    refetch: () => Promise<unknown> | void;
    client: ApolloClient<object>;
    onSuccess?: (actorUrn: string, newRoleUrn: string) => void;
    onPostRefetch?: (actorUrn: string) => void;
    onCancel?: (actorUrn: string, originalRoleUrn: string) => void;
    onError?: (actorUrn: string, originalRoleUrn: string) => void;
}

export function useRoleAssignment({
    entityLabel,
    selectRoleOptions,
    refetch,
    client,
    onSuccess,
    onPostRefetch,
    onCancel,
    onError,
}: UseRoleAssignmentOptions) {
    const [roleAssignmentState, setRoleAssignmentState] = useState<RoleAssignmentState | null>(null);
    const [batchAssignRoleMutation] = useBatchAssignRoleMutation();

    const onSelectRole = useCallback(
        (actorUrn: string, actorName: string, newRoleUrn: string, originalRoleUrn: string) => {
            setRoleAssignmentState({
                isViewingAssignRole: true,
                actorUrn,
                actorName,
                currentRoleUrn: newRoleUrn,
                originalRoleUrn,
            });
        },
        [],
    );

    const onCancelRoleAssignment = useCallback(() => {
        if (roleAssignmentState && onCancel) {
            onCancel(roleAssignmentState.actorUrn, roleAssignmentState.originalRoleUrn);
        }
        setRoleAssignmentState(null);
    }, [roleAssignmentState, onCancel]);

    const onConfirmRoleAssignment = useCallback(() => {
        if (!roleAssignmentState) return;

        const roleToAssign = selectRoleOptions.find((role) => role.urn === roleAssignmentState.currentRoleUrn);
        const newRoleUrn = roleToAssign?.urn || NO_ROLE_URN;

        const isRemoval = newRoleUrn === NO_ROLE_URN;
        const successMsg = isRemoval
            ? i18next.t('roleAssignment.removeSuccess', {
                  ns: 'entity.identity',
                  entityLabel,
                  name: roleAssignmentState.actorName,
              })
            : i18next.t('roleAssignment.assignSuccess', {
                  ns: 'entity.identity',
                  role: roleToAssign?.name,
                  entityLabel,
                  name: roleAssignmentState.actorName,
              });

        batchAssignRoleMutation({
            variables: {
                input: {
                    roleUrn: isRemoval ? null : newRoleUrn,
                    actors: [roleAssignmentState.actorUrn],
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    toast.success(successMsg);

                    onSuccess?.(roleAssignmentState.actorUrn, newRoleUrn);
                    const { actorUrn } = roleAssignmentState;
                    setRoleAssignmentState(null);

                    setTimeout(() => {
                        clearRoleListCache(client);
                        Promise.resolve(refetch())
                            .then(() => onPostRefetch?.(actorUrn))
                            .catch((err) => console.error('Failed to refetch after role assignment:', err));
                    }, 3000);
                }
            })
            .catch((e) => {
                onError?.(roleAssignmentState.actorUrn, roleAssignmentState.originalRoleUrn);
                const failureMsg = isRemoval
                    ? i18next.t('roleAssignment.removeError', {
                          ns: 'entity.identity',
                          entityLabel,
                          name: roleAssignmentState.actorName,
                          error: e.message || '',
                      })
                    : i18next.t('roleAssignment.assignError', {
                          ns: 'entity.identity',
                          role: roleToAssign?.name,
                          entityLabel,
                          name: roleAssignmentState.actorName,
                          error: e.message || '',
                      });
                toast.error(failureMsg);
            });
    }, [
        roleAssignmentState,
        selectRoleOptions,
        batchAssignRoleMutation,
        entityLabel,
        client,
        refetch,
        onSuccess,
        onPostRefetch,
        onError,
    ]);

    const getRoleAssignmentMessage = useCallback(() => {
        if (!roleAssignmentState) return '';
        const roleToAssign = selectRoleOptions.find((role) => role.urn === roleAssignmentState.currentRoleUrn);
        return roleToAssign?.urn === NO_ROLE_URN || !roleToAssign
            ? i18next.t('roleAssignment.removeMessage', {
                  ns: 'entity.identity',
                  name: roleAssignmentState.actorName,
              })
            : i18next.t('roleAssignment.assignMessage', {
                  ns: 'entity.identity',
                  role: roleToAssign?.name,
                  name: roleAssignmentState.actorName,
              });
    }, [roleAssignmentState, selectRoleOptions]);

    return {
        roleAssignmentState,
        onSelectRole,
        onCancelRoleAssignment,
        onConfirmRoleAssignment,
        getRoleAssignmentMessage,
    };
}
