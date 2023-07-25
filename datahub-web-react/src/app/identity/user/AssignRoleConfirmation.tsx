import React from 'react';
import { message, Popconfirm } from 'antd';
import { useBatchAssignRoleMutation } from '../../../graphql/mutations.generated';
import { DataHubRole } from '../../../types.generated';
import analytics, { EventType } from '../../analytics';

type Props = {
    visible: boolean;
    roleToAssign: DataHubRole | undefined;
    userUrn: string;
    username: string;
    onClose: () => void;
    onConfirm: () => void;
};

export default function AssignRoleConfirmation({
    visible,
    roleToAssign,
    userUrn,
    username,
    onClose,
    onConfirm,
}: Props) {
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
                            ? `分配角色 ${roleToAssign?.name} 给用户 ${username}!`
                            : `为用户${username} 移除角色!`,
                        duration: 2,
                    });
                    onConfirm();
                }
            })
            .catch((e) => {
                message.destroy();
                message.error({
                    content: roleToAssign
                        ? `分配角色 ${roleToAssign?.name} 给用户 ${username}时失败，失败信息: \n ${e.message || ''}`
                        : `为用户 ${username} 移除角色时失败，失败信息: \n ${e.message || ''}`,
                    duration: 3,
                });
            });
    };

    const assignRoleText = roleToAssign
        ? `您确认要将角色 ${roleToAssign?.name} 分配给该用户 ${username} 吗?`
        : `您确认要将该用户 ${username} 的角色移除吗?`;

    return <Popconfirm title={assignRoleText} visible={visible} onConfirm={batchAssignRole} onCancel={onClose} />;
}
