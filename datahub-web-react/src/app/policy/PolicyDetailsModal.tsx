import React from 'react';
import { Button, Col, Divider, Modal, Row, Tag, Typography } from 'antd';
import styled from 'styled-components';
import { useEntityRegistry } from '../useEntityRegistry';
import { EntityType, Maybe, PolicyState, PolicyType } from '../../types.generated';
import { useAppConfig } from '../useAppConfig';
import { mapResourceTypeToDisplayName } from './policyUtils';
import { CustomAvatar } from '../shared/avatar';

type PrivilegeOptionType = {
    type?: string;
    name?: Maybe<string>;
};

type Props = {
    policy: any;
    visible: boolean;
    onClose: () => void;
    privileges: PrivilegeOptionType[] | undefined;
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

const PrivilegeTag = styled(Tag)`
    && {
        border-radius: 2px !important;
    }
`;
const Privileges = styled.div`
    & ${PrivilegeTag}:nth-child(n+1) {
        margin-top: 5px !important;
    }
`;

/**
 * Component used for displaying the details about an existing Policy.
 *
 * TODO: Use the "display names" when rendering privileges, instead of raw privilege type.
 */
export default function PolicyDetailsModal({ policy, visible, onClose, privileges }: Props) {
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
                    <Tag color={isActive ? 'green' : 'red'}>{policy?.state}</Tag>
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
                <Privileges>
                    <Typography.Title level={5}>Privileges</Typography.Title>
                    <ThinDivider />
                    {privileges?.map((priv) => (
                        <PrivilegeTag>{priv?.name}</PrivilegeTag>
                    ))}
                </Privileges>
                <div>
                    <Typography.Title level={5}>Applies to Owners</Typography.Title>
                    <ThinDivider />
                    <PoliciesTag>{policy?.resourceOwners ? 'True' : 'False'}</PoliciesTag>
                </div>
                <div>
                    <Typography.Title level={5}>Applies to Users</Typography.Title>
                    <ThinDivider />
                    <Row>
                        <Col flex="auto">
                            {policy?.resolvedUsers?.map((user) => (
                                <CustomAvatar
                                    size={28}
                                    name={user?.username}
                                    url={`/${entityRegistry.getPathName(EntityType.CorpUser)}/${user.urn}`}
                                    photoUrl={
                                        user?.editableProperties?.pictureLink ||
                                        user?.editableInfo?.pictureLink ||
                                        undefined
                                    }
                                />
                            ))}
                        </Col>
                        <Col flex={1} style={{ display: 'flex', justifyContent: 'end' }}>
                            {policy?.allUsers ? <Tag>All Users</Tag> : null}
                        </Col>
                    </Row>
                </div>
                <div>
                    <Typography.Title level={5}>Applies to Groups</Typography.Title>
                    <ThinDivider />
                    <Row>
                        <Col flex="auto">
                            {policy?.resolvedGroups?.map((group) => (
                                <CustomAvatar
                                    size={28}
                                    name={group?.name}
                                    url={`/${entityRegistry.getPathName(EntityType.CorpGroup)}/${group.urn}`}
                                    isGroup
                                />
                            ))}
                        </Col>
                        <Col flex={1} style={{ display: 'flex', justifyContent: 'end' }}>
                            {policy?.allGroups ? <Tag>All Groups</Tag> : null}
                        </Col>
                    </Row>
                </div>
            </PolicyContainer>
        </Modal>
    );
}
