import React from 'react';
import { message, Popconfirm } from 'antd';
import { useTranslation } from 'react-i18next';
import { useBatchAssignRoleMutation } from '../../../graphql/mutations.generated';
import { DataHubRole } from '../../../types.generated';
import analytics, { EventType } from '../../analytics';

type Props = {
    open: boolean;
    roleToAssign: DataHubRole | undefined;
    userUrn: string;
    username: string;
    onClose: () => void;
    onConfirm: () => void;
};

export default function AssignRoleConfirmation({ open, roleToAssign, userUrn, username, onClose, onConfirm }: Props) {
    const { t } = useTranslation();
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
                            ? t('user.assignRoleSuccess', { roleName: roleToAssign?.name, username })
                            : t('user.removeRoleSuccess', { username }),
                        duration: 2,
                    });
                    onConfirm();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: roleToAssign
                        ? `${t('user.assignRoleError', { roleName: roleToAssign?.name, username })}: \n ${
                              e.message || ''
                          }`
                        : `${t('user.removeRoleError', { username })}: \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };

    const assignRoleText = roleToAssign
        ? `${t('user.assignRoleTitle', { roleName: roleToAssign?.name, username })}?`
        : `${t('user.removeRoleTitle', { username })}?`;

    return <Popconfirm title={assignRoleText} open={open} onConfirm={batchAssignRole} onCancel={onClose} />;
}
