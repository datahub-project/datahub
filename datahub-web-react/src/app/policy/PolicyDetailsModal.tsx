import React from 'react';
import { Button, Divider, Modal, Row, Space, Tag, Typography } from 'antd';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { useEntityRegistry } from '../useEntityRegistry';
import { EntityType, Policy, PolicyType } from '../../types.generated';
import { useAppConfig } from '../useAppConfig';
import { mapResourceTypeToDisplayName } from './policyUtils';

type Props = {
    policy: Omit<Policy, 'urn'>;
    visible: boolean;
    onEdit: () => void;
    onClose: () => void;
    onRemove: () => void;
    onToggleActive: (value: boolean) => void;
};

const ThinDivider = styled(Divider)`
    margin-top: 8px;
    margin-bottom: 8px;
`;

// TODO: Cleanup styling.
export default function PolicyDetailsModal({ policy, visible, onEdit, onClose, onRemove, onToggleActive }: Props) {
    const isActive = policy.state === 'ACTIVE';
    const isMetadataPolicy = policy.type === PolicyType.Metadata;

    const entityRegistry = useEntityRegistry();

    const {
        config: { policiesConfig },
    } = useAppConfig();

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

    return (
        <Modal title={policy.name} visible={visible} onCancel={onClose} closable width={800} footer={actionButtons}>
            <Row style={{ paddingLeft: 20, paddingRight: 20 }}>
                <Space direction="vertical" size="large">
                    <div>
                        <Typography.Title level={5}>Type</Typography.Title>
                        <ThinDivider />
                        <Tag>{policy.type}</Tag>
                    </div>
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
                    {isMetadataPolicy && (
                        <>
                            <div>
                                <Typography.Title level={5}>Asset Type</Typography.Title>
                                <ThinDivider />
                                <Tag>
                                    {mapResourceTypeToDisplayName(
                                        policy.resources?.type || '',
                                        policiesConfig.resourcePrivileges || [],
                                    )}
                                </Tag>
                            </div>
                            <div>
                                <Typography.Title level={5}>Assets</Typography.Title>
                                <ThinDivider />
                                {policy.resources?.resources?.map((urn) => {
                                    // TODO: Wrap in a link for entities.
                                    return (
                                        <Tag>
                                            <Typography.Text>{urn}</Typography.Text>
                                        </Tag>
                                    );
                                })}
                                {policy.resources?.allResources && <Tag>All</Tag>}
                            </div>
                        </>
                    )}

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
                        <Tag>{policy.actors.resourceOwners ? 'True' : 'False'}</Tag>
                    </div>
                    <div>
                        <Typography.Title level={5}>Applies to Users</Typography.Title>
                        <ThinDivider />
                        {policy.actors.users?.map((userUrn) => (
                            <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${userUrn}`} key={userUrn}>
                                <Tag>
                                    <Typography.Text underline>{userUrn}</Typography.Text>
                                </Tag>
                            </Link>
                        ))}
                        {policy.actors.allUsers && <Tag>All</Tag>}
                    </div>
                    <div>
                        <Typography.Title level={5}>Applies to Groups</Typography.Title>
                        <ThinDivider />
                        {policy.actors.groups?.map((groupUrn) => (
                            <Link
                                to={`/${entityRegistry.getPathName(EntityType.CorpGroup)}/${groupUrn}`}
                                key={groupUrn}
                            >
                                <Tag>
                                    <Typography.Text underline>{groupUrn}</Typography.Text>
                                </Tag>
                            </Link>
                        ))}
                        {policy.actors.allGroups && <Tag>All</Tag>}
                    </div>
                </Space>
            </Row>
        </Modal>
    );
}
