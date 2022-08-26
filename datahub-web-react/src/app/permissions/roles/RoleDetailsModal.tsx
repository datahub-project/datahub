import React from 'react';
import { Button, Divider, Modal, Typography } from 'antd';
import styled from 'styled-components';
import { useEntityRegistry } from '../../useEntityRegistry';
import { CorpUser, EntityType, Policy, Role } from '../../../types.generated';
import AvatarsGroup from '../AvatarsGroup';

type Props = {
    role: Role;
    visible: boolean;
    onClose: () => void;
};

const PolicyContainer = styled.div`
    padding-left: 20px;
    padding-right: 20px;
    > div {
        margin-bottom: 32px;
    }
`;

const ButtonsContainer = styled.div`
    display: flex;
    width: 100%;
    justify-content: flex-end;
    align-items: center;
`;

const ThinDivider = styled(Divider)`
    margin-top: 8px;
    margin-bottom: 8px;
`;

/**
 * Component used for displaying the details about an existing Role.
 */
export default function RoleDetailsModal({ role, visible, onClose }: Props) {
    const entityRegistry = useEntityRegistry();

    const actionButtons = (
        <ButtonsContainer>
            <Button onClick={onClose}>Close</Button>
        </ButtonsContainer>
    );

    const users = role?.relationships?.relationships
        .filter((relationship) => relationship?.entity?.type === EntityType.CorpUser)
        .map((relationship) => relationship.entity as CorpUser);
    const policies = role?.relationships?.relationships
        .filter((relationship) => relationship?.entity?.type === EntityType.DatahubPolicy)
        .map((relationship) => relationship.entity as Policy);

    return (
        <Modal title={role?.name} visible={visible} onCancel={onClose} closable width={800} footer={actionButtons}>
            <PolicyContainer>
                <div>
                    <Typography.Title level={5}>Description</Typography.Title>
                    <ThinDivider />
                    <Typography.Text type="secondary">{role?.description}</Typography.Text>
                </div>
                <div>
                    <Typography.Title level={5}>Users</Typography.Title>
                    <ThinDivider />
                    <AvatarsGroup users={users} entityRegistry={entityRegistry} maxCount={50} size={28} />
                </div>
                <div>
                    <Typography.Title level={5}>Associated Policies</Typography.Title>
                    <ThinDivider />
                    <AvatarsGroup policies={policies} entityRegistry={entityRegistry} maxCount={50} size={28} />
                </div>
            </PolicyContainer>
        </Modal>
    );
}
