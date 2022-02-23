import React from 'react';
import { Button, Divider, Modal, Tag, Typography } from 'antd';
import styled from 'styled-components';
import { Link } from 'react-router-dom';
import { useEntityRegistry } from '../useEntityRegistry';
import { EntityType, Policy, PolicyState, PolicyType } from '../../types.generated';
import { useAppConfig } from '../useAppConfig';
import { mapResourceTypeToDisplayName } from './policyUtils';

type Props = {
    policy: Omit<Policy, 'urn'>;
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

const PoliciesTag = styled(Tag)`
    && {
        border-radius: 2px !important;
    }
`;

/**
 * Component used for displaying the details about an existing Policy.
 *
 * TODO: Use the "display names" when rendering privileges, instead of raw privilege type.
 */
export default function PolicyDetailsModal({ policy, visible, onClose }: Props) {
    const entityRegistry = useEntityRegistry();

    const isActive = policy?.state === PolicyState.Active;
    const isMetadataPolicy = policy?.type === PolicyType.Metadata;

    const {
        config: { policiesConfig },
    } = useAppConfig();

    const actionButtons = (
        <ButtonsContainer>
            <Button onClick={onClose}>Cancel</Button>
        </ButtonsContainer>
    );

    return (
        <Modal title={policy?.name} visible={visible} onCancel={onClose} closable width={800} footer={actionButtons}>
            <PolicyContainer>
                <div>
                    <Typography.Title level={5}>Type</Typography.Title>
                    <ThinDivider />
                    <PoliciesTag>{policy?.type}</PoliciesTag>
                </div>
                <div>
                    <Typography.Title level={5}>State</Typography.Title>
                    <ThinDivider />
                    <PoliciesTag color={isActive ? 'green' : 'red'}>{policy?.state}</PoliciesTag>
                </div>
                <div>
                    <Typography.Title level={5}>Description</Typography.Title>
                    <ThinDivider />
                    <Typography.Text type="secondary">{policy?.description}</Typography.Text>
                </div>
                {isMetadataPolicy && (
                    <>
                        <div>
                            <Typography.Title level={5}>Asset Type</Typography.Title>
                            <ThinDivider />
                            <PoliciesTag>
                                {mapResourceTypeToDisplayName(
                                    policy?.resources?.type || '',
                                    policiesConfig?.resourcePrivileges || [],
                                )}
                            </PoliciesTag>
                        </div>
                        <div>
                            <Typography.Title level={5}>Assets</Typography.Title>
                            <ThinDivider />
                            {policy?.resources?.resources?.map((urn) => {
                                // TODO: Wrap in a link for entities.
                                return (
                                    <PoliciesTag>
                                        <Typography.Text>{urn}</Typography.Text>
                                    </PoliciesTag>
                                );
                            })}
                            {policy?.resources?.allResources && <PoliciesTag>All</PoliciesTag>}
                        </div>
                    </>
                )}
                <div>
                    <Typography.Title level={5}>Privileges</Typography.Title>
                    <ThinDivider />
                    {policy?.privileges?.map((priv) => (
                        <PoliciesTag>{priv}</PoliciesTag>
                    ))}
                </div>
                <div>
                    <Typography.Title level={5}>Applies to Owners</Typography.Title>
                    <ThinDivider />
                    <PoliciesTag>{policy?.actors?.resourceOwners ? 'True' : 'False'}</PoliciesTag>
                </div>
                <div>
                    <Typography.Title level={5}>Applies to Users</Typography.Title>
                    <ThinDivider />
                    {policy?.actors?.users?.map((userUrn) => (
                        <Link to={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${userUrn}`} key={userUrn}>
                            <PoliciesTag>
                                <Typography.Text underline>{userUrn}</Typography.Text>
                            </PoliciesTag>
                        </Link>
                    ))}
                    {policy?.actors?.allUsers && <PoliciesTag>All</PoliciesTag>}
                </div>
                <div>
                    <Typography.Title level={5}>Applies to Groups</Typography.Title>
                    <ThinDivider />
                    {policy?.actors?.groups?.map((groupUrn) => (
                        <Link to={`/${entityRegistry.getPathName(EntityType.CorpGroup)}/${groupUrn}`} key={groupUrn}>
                            <PoliciesTag>
                                <Typography.Text underline>{groupUrn}</Typography.Text>
                            </PoliciesTag>
                        </Link>
                    ))}
                    {policy?.actors?.allGroups && <PoliciesTag>All</PoliciesTag>}
                </div>
            </PolicyContainer>
        </Modal>
    );
}
