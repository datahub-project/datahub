import { Popconfirm, message } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import analytics, { EventType } from '@app/analytics';

import { useBatchAssignRoleMutation } from '@graphql/mutations.generated';
import { DataHubRole } from '@types';

type Props = {
    open: boolean;
    roleToAssign: DataHubRole | undefined;
    userUrn: string;
    username: string;
    onClose: () => void;
    onConfirm: () => void;
};

export default function AssignRoleConfirmation({ open, roleToAssign, userUrn, username, onClose, onConfirm }: Props) {
    const { t } = useTranslation('entity.identity');
    const [batchAssignRoleMutation] = useBatchAssignRoleMutation();
    // eslint-disable-next-line
    const batchAssignRole = () => {
        batchAssignRoleMutation({
            variables: {
                input: {
                    roleUrn: roleToAssign?.urn,
                    actors: [userUrn],
                },
            },
        })
            .then(({ errors }) => {
                if (!errors) {
                    analytics.event({
                        type: EventType.SelectUserRoleEvent,
                        roleUrn: roleToAssign?.urn || 'undefined',
                        userUrn,
                    });
                    message.success({
                        content: roleToAssign
                            ? t('users.roleAssign.assignSuccess', { role: roleToAssign?.name, name: username })
                            : t('users.roleAssign.removeSuccess', { name: username }),
                        duration: 2,
                    });
                    onConfirm();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: roleToAssign
                        ? t('users.roleAssign.assignError', {
                              role: roleToAssign?.name,
                              name: username,
                              error: e.message || '',
                          })
                        : t('users.roleAssign.removeError', { name: username, error: e.message || '' }),
                    duration: 3,
                });
            });
    };

    const assignRoleText = roleToAssign
        ? t('roleAssignment.assignMessage', { role: roleToAssign?.name, name: username })
        : t('roleAssignment.removeMessage', { name: username });

    return <Popconfirm title={assignRoleText} open={open} onConfirm={batchAssignRole} onCancel={onClose} />;
}
