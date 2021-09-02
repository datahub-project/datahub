import React from 'react';
import { Button, Divider, Modal, Tag, Typography } from 'antd';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { useEntityRegistry } from '../useEntityRegistry';
import { EntityType, Policy, PolicyState, PolicyType } from '../../types.generated';
import { useAppConfig } from '../useAppConfig';
import { mapResourceTypeToDisplayName } from './policyUtils';
import { ANTD_GRAY } from '../entity/shared/constants';

type Props = {
    policy: Omit<Policy, 'urn'>;
    visible: boolean;
    onEdit: () => void;
    onClose: () => void;
    onRemove: () => void;
    onToggleActive: (value: boolean) => void;
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
 * Component used for displaying the details about an existing Policy.
 *
 * TODO: Use the "display names" when rendering privileges, instead of raw privilege type.
 */
export default function PolicyDetailsModal({ policy, visible, onEdit, onClose, onRemove, onToggleActive }: Props) {
    const entityRegistry = useEntityRegistry();

    const isActive = policy.state === PolicyState.Active;
    const isMetadataPolicy = policy.type === PolicyType.Metadata;
    const isEditable = policy.editable; // Whether we should show edit buttons.

    const {
        config: { policiesConfig },
    } = useAppConfig();

    const activeActionButton = isActive ? (
        <Button
            disabled={!isEditable}
            onClick={() => onToggleActive(false)}
            style={{ color: isEditable ? 'red' : ANTD_GRAY[6] }}
        >
            Deactivate
        </Button>
    ) : (
        <Button
            disabled={!isEditable}
            onClick={() => onToggleActive(true)}
            style={{ color: isEditable ? 'green' : ANTD_GRAY[6] }}
        >
            Activate
        </Button>
    );

    const actionButtons = (
        <ButtonsContainer>
            <Button disabled={!isEditable} style={{ color: !isEditable ? ANTD_GRAY[6] : undefined }} onClick={onEdit}>
                Edit
            </Button>
            {activeActionButton}
            <Button disabled={!isEditable} style={{ color: isEditable ? 'red' : ANTD_GRAY[6] }} onClick={onRemove}>
                Delete
            </Button>
            <Button onClick={onClose}>Cancel</Button>
        </ButtonsContainer>
    );

    return (
        <Modal title={policy.name} visible={visible} onCancel={onClose} closable width={800} footer={actionButtons}>
            <PolicyContainer>
                <div>
                    <Typography.Title level={5}>Type</Typography.Title>
                    <ThinDivider />
                    <Tag>{policy.type}</Tag>
                </div>
                <div>
                    <Typography.Title level={5}>State</Typography.Title>
                    <ThinDivider />
                    <Tag color={isActive ? 'green' : 'red'}>{policy.state}</Tag>
                </div>
                <div>
                    <Typography.Title level={5}>Description</Typography.Title>
                    <ThinDivider />
                    <Typography.Text type="secondary">{policy.description}</Typography.Text>
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
                        <Link to={`/${entityRegistry.getPathName(EntityType.CorpGroup)}/${groupUrn}`} key={groupUrn}>
                            <Tag>
                                <Typography.Text underline>{groupUrn}</Typography.Text>
                            </Tag>
                        </Link>
                    ))}
                    {policy.actors.allGroups && <Tag>All</Tag>}
                </div>
            </PolicyContainer>
        </Modal>
    );
}
