import React from 'react';
import { Link } from 'react-router-dom';
import { Button, Divider, Modal, Tag, Typography } from 'antd';
import styled from 'styled-components';
import { useEntityRegistry } from '../../useEntityRegistry';
import { Maybe, Policy, PolicyMatchCondition, PolicyState, PolicyType } from '../../../types.generated';
import { useAppConfig } from '../../useAppConfig';
import {
    convertLegacyResourceFilter,
    getFieldValues,
    getFieldCondition,
    mapResourceTypeToDisplayName,
} from './policyUtils';
import AvatarsGroup from '../AvatarsGroup';
import { RESOURCE_TYPE, RESOURCE_URN, TYPE, URN } from './constants';

type PrivilegeOptionType = {
    type?: string;
    name?: Maybe<string>;
};

type Props = {
    policy: Omit<Policy, 'urn'>;
    open: boolean;
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
 */
export default function PolicyDetailsModal({ policy, open, onClose, privileges }: Props) {
    const entityRegistry = useEntityRegistry();

    const isActive = policy?.state === PolicyState.Active;
    const isMetadataPolicy = policy?.type === PolicyType.Metadata;

    const resources = convertLegacyResourceFilter(policy?.resources);
    const resourceTypes = getFieldValues(resources?.filter, TYPE, RESOURCE_TYPE) || [];
    const dataPlatformInstances = getFieldValues(resources?.filter, 'DATA_PLATFORM_INSTANCE') || [];
    const resourceEntities = getFieldValues(resources?.filter, URN, RESOURCE_URN) || [];
    const resourceFilterCondition =
        getFieldCondition(resources?.filter, URN, RESOURCE_URN) || PolicyMatchCondition.Equals;
    const domains = getFieldValues(resources?.filter, 'DOMAIN') || [];

    const {
        config: { policiesConfig },
    } = useAppConfig();

    const actionButtons = (
        <ButtonsContainer>
            <Button onClick={onClose}>Close</Button>
        </ButtonsContainer>
    );

    const getDisplayName = (entity) => {
        if (!entity) {
            return null;
        }
        return entityRegistry.getDisplayName(entity.type, entity);
    };

    const getEntityTag = (criterionValue) => {
        return (
            (criterionValue.entity && (
                <Link
                    target="_blank"
                    rel="noopener noreferrer"
                    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
                    to={() => `${entityRegistry.getEntityUrl(criterionValue.entity!.type, criterionValue.value)}`}
                >
                    {getDisplayName(criterionValue.entity)}
                </Link>
            )) || <Typography.Text>{criterionValue.value}</Typography.Text>
        );
    };

    const getWildcardUrnTag = (criterionValue) => {
        return <Typography.Text>{criterionValue.value}*</Typography.Text>;
    };

    const resourceOwnersField = (actors) => {
        if (!actors?.resourceOwners) {
            return <PoliciesTag>No</PoliciesTag>;
        }
        if ((actors?.resolvedOwnershipTypes?.length ?? 0) > 0) {
            return (
                <div>
                    {actors?.resolvedOwnershipTypes?.map((type) => (
                        <PoliciesTag>{type.info.name}</PoliciesTag>
                    ))}
                </div>
            );
        }
        return <PoliciesTag>Yes - All owners</PoliciesTag>;
    };

    return (
        <Modal title={policy?.name} open={open} onCancel={onClose} closable width={800} footer={actionButtons}>
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
                            {(resourceTypes?.length &&
                                resourceTypes.map((value, key) => {
                                    return (
                                        // eslint-disable-next-line react/no-array-index-key
                                        <PoliciesTag key={`type-${value.value}-${key}`}>
                                            <Typography.Text>
                                                {mapResourceTypeToDisplayName(
                                                    value.value,
                                                    policiesConfig?.resourcePrivileges || [],
                                                )}
                                            </Typography.Text>
                                        </PoliciesTag>
                                    );
                                })) || <PoliciesTag>All</PoliciesTag>}
                        </div>
                        <div>
                            <Typography.Title level={5}>Assets</Typography.Title>
                            <ThinDivider />
                            {(resourceEntities?.length &&
                                resourceEntities.map((value, key) => {
                                    return (
                                        // eslint-disable-next-line react/no-array-index-key
                                        <PoliciesTag key={`resource-${value.value}-${key}`}>
                                            {resourceFilterCondition &&
                                            resourceFilterCondition === PolicyMatchCondition.StartsWith
                                                ? getWildcardUrnTag(value)
                                                : getEntityTag(value)}
                                        </PoliciesTag>
                                    );
                                })) || <PoliciesTag>All</PoliciesTag>}
                        </div>
                        {dataPlatformInstances?.length > 0 && (
                            <div>
                                <Typography.Title level={5}>Data Platform Instances</Typography.Title>
                                <ThinDivider />
                                {dataPlatformInstances.map((value, key) => {
                                    return (
                                        // eslint-disable-next-line react/no-array-index-key
                                        <PoliciesTag key={`dataPlatformInstance-${value.value}-${key}`}>
                                            <Typography.Text>{getDisplayName(value.entity)}</Typography.Text>
                                        </PoliciesTag>
                                    );
                                })}
                            </div>
                        )}
                        <div>
                            <Typography.Title level={5}>Domains</Typography.Title>
                            <ThinDivider />
                            {(domains?.length &&
                                domains.map((value, key) => {
                                    return (
                                        // eslint-disable-next-line react/no-array-index-key
                                        <PoliciesTag key={`domain-${value.value}-${key}`}>
                                            {getEntityTag(value)}
                                        </PoliciesTag>
                                    );
                                })) || <PoliciesTag>All</PoliciesTag>}
                        </div>
                    </>
                )}
                <Privileges>
                    <Typography.Title level={5}>Privileges</Typography.Title>
                    <ThinDivider />
                    {privileges?.map((priv, key) => (
                        // eslint-disable-next-line react/no-array-index-key
                        <PrivilegeTag key={`${priv}-${key}`}>{priv?.name}</PrivilegeTag>
                    ))}
                </Privileges>
                <div>
                    <Typography.Title level={5}>Applies to Owners</Typography.Title>
                    <ThinDivider />
                    {resourceOwnersField(policy?.actors)}
                </div>
                <div>
                    <Typography.Title level={5}>Applies to Users</Typography.Title>
                    <ThinDivider />
                    <AvatarsGroup
                        users={policy?.actors?.resolvedUsers}
                        entityRegistry={entityRegistry}
                        maxCount={50}
                        size={28}
                    />
                    {policy?.actors?.allUsers ? <Tag>All Users</Tag> : null}
                </div>
                <div>
                    <Typography.Title level={5}>Applies to Groups</Typography.Title>
                    <ThinDivider />
                    <AvatarsGroup
                        groups={policy?.actors?.resolvedGroups}
                        entityRegistry={entityRegistry}
                        maxCount={50}
                        size={28}
                    />
                    {policy?.actors?.allGroups ? <Tag>All Groups</Tag> : null}
                </div>
                <div>
                    <Typography.Title level={5}>Applies to Roles</Typography.Title>
                    <ThinDivider />
                    <AvatarsGroup
                        roles={policy?.actors?.resolvedRoles}
                        entityRegistry={entityRegistry}
                        maxCount={50}
                        size={28}
                    />
                </div>
            </PolicyContainer>
        </Modal>
    );
}
