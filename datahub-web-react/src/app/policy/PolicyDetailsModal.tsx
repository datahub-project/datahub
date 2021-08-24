import React from 'react';
import { Button, Divider, Modal, Row, Space, Tag, Typography } from 'antd';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { useEntityRegistry } from '../useEntityRegistry';
import { EntityType } from '../../types.generated';

type Props = {
    policy: any;
    visible: boolean;
    onEdit: () => void;
    onClose: () => void;
    onRemove: () => void;
    onToggleActive: (value: boolean) => void;
};

const ThinDivider = styled(Divider)`
    margin: 8px;
`;

// TODO: Cleanup styling.
// TODO: Actually show the users, groups, and resources the policy applies to. (With links)
// Ask if you're sure you want to delete a policy before deleting.//

// TODO: Handle "all" case for all users, resources, groups.
// TODO: Display name functionality for Entity.
export default function PolicyDetailsModal({ policy, visible, onEdit, onClose, onRemove, onToggleActive }: Props) {
    const isActive = policy.state === 'ACTIVE';

    const activeActionButton = isActive ? (
        <Button onClick={() => onToggleActive(false)} style={{ color: 'red' }}>
            Deactivate
        </Button>
    ) : (
        <Button onClick={() => onToggleActive(true)} style={{ color: 'green' }}>
            Activate
        </Button>
    );

    const actionButtons = (
        <Space direction="horizontal">
            <Button onClick={onEdit}>Edit</Button>
            {activeActionButton}
            <Button style={{ color: 'red' }} onClick={onRemove}>
                Delete
            </Button>
            <Button onClick={onClose}>Cancel</Button>
        </Space>
    );

    const entityRegistry = useEntityRegistry();

    return (
        <Modal title={policy.name} visible={visible} onCancel={onClose} closable width={800} footer={actionButtons}>
            <Row style={{ paddingLeft: 20, paddingRight: 20 }}>
                <Space direction="vertical" size="large">
                    <div>
                        <Typography.Title level={5}>Description</Typography.Title>
                        <ThinDivider />
                        <Typography.Text type="secondary">{policy.description}</Typography.Text>
                    </div>
                    <div>
                        <Typography.Title level={5}>State</Typography.Title>
                        <ThinDivider />
                        <Tag color={isActive ? 'green' : 'red'}>{policy.state}</Tag>
                    </div>
                    <div>
                        <Typography.Title level={5}>Asset Type</Typography.Title>
                        <ThinDivider />
                        <Tag>{entityRegistry.getCollectionName(policy.resource.type)}</Tag>
                    </div>

                    <div>
                        <Typography.Title level={5}>Assets</Typography.Title>
                        <ThinDivider />
                        {policy.resource.urns.map((urn) => (
                            <Link to={`/${entityRegistry.getPathName(policy.resource.type)}/${urn}`} key={urn}>
                                <Tag>
                                    <Typography.Text underline>{urn}</Typography.Text>
                                </Tag>
                            </Link>
                        ))}
                    </div>

                    <div>
                        <Typography.Title level={5}>Privileges</Typography.Title>
                        <ThinDivider />
                        {policy.privileges.map((priv) => (
                            <Tag>{priv}</Tag>
                        ))}
                    </div>
                    <div>
                        <Typography.Title level={5}>Applies to Owners</Typography.Title>
                        <ThinDivider />
                        <Tag>{policy.actors.appliesToOwners ? 'True' : 'False'}</Tag>
                    </div>
                    <div>
                        <Typography.Title level={5}>Applies to Users</Typography.Title>
                        <ThinDivider />
                        {policy.actors.users.map((userUrn) => (
                            <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${userUrn}`} key={userUrn}>
                                <Tag>
                                    <Typography.Text underline>{userUrn}</Typography.Text>
                                </Tag>
                            </Link>
                        ))}
                    </div>
                    <div>
                        <Typography.Title level={5}>Applies to Groups</Typography.Title>
                        <ThinDivider />
                        {policy.actors.groups.map((groupUrn) => (
                            <Link
                                to={`/${entityRegistry.getPathName(EntityType.CorpGroup)}/${groupUrn}`}
                                key={groupUrn}
                            >
                                <Tag>
                                    <Typography.Text underline>{groupUrn}</Typography.Text>
                                </Tag>
                            </Link>
                        ))}
                    </div>
                </Space>
            </Row>
        </Modal>
    );
}
