import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { Modal, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';
import { SetDomainModal } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SetDomainModal';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { ENTITY_PROFILE_DOMAINS_ID } from '@app/onboarding/config/EntityProfileOnboardingConfig';
import ProposalModal from '@app/shared/tags/ProposalModal';
import { DomainContent, DomainLink } from '@app/sharedV2/tags/DomainLink';
import { colors } from '@src/alchemy-components';
import ProposedIcon from '@src/app/entityV2/shared/sidebarSection/ProposedIcon';
import { getProposedItemsByType } from '@src/app/entityV2/shared/utils';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { ActionRequest, ActionRequestType, EntityType } from '@src/types.generated';

import { useUnsetDomainMutation } from '@graphql/mutations.generated';

const Content = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    flex-wrap: wrap;
    text-wrap: wrap;
    gap: 8px;
`;

const DomainLinkWrapper = styled.div`
    display: flex;
    align-items: center;
`;

const ProposedDomain = styled.div`
    display: flex;
    align-items: center;
    padding: 3px;
    border-radius: 12px;
    border: 1px dashed ${colors.gray[200]};
    color: ${colors.gray[500]};

    :hover {
        cursor: pointer;
    }
`;

interface PropertiesProps {
    updateOnly?: boolean;
}

interface Props {
    readOnly?: boolean;
    properties?: PropertiesProps;
}

export const SidebarDomainSection = ({ readOnly, properties }: Props) => {
    const updateOnly = properties?.updateOnly;
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistryV2();
    const refetch = useRefetch();
    const urn = useMutationUrn();
    const [unsetDomainMutation] = useUnsetDomainMutation();
    const [showModal, setShowModal] = useState(false);
    const domain = entityData?.domain?.domain;

    const [selectedActionRequest, setSelectedActionRequest] = useState<ActionRequest | undefined | null>(null);

    const proposedDomainRequests = getProposedItemsByType(
        entityData?.proposals || [],
        ActionRequestType.DomainAssociation,
    );

    const canEditDomains = !!entityData?.privileges?.canEditDomains;
    const canProposeDomains = !!entityData?.privileges?.canProposeDomains;

    const removeDomain = (urnToRemoveFrom) => {
        unsetDomainMutation({ variables: { entityUrn: urnToRemoveFrom } })
            .then(() => {
                message.success({ content: 'Removed Domain.', duration: 2 });
                refetch?.();
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to remove domain: \n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    const onRemoveDomain = (urnToRemoveFrom) => {
        Modal.confirm({
            title: `Confirm Domain Removal`,
            content: `Are you sure you want to remove this domain?`,
            onOk() {
                removeDomain(urnToRemoveFrom);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <div id={ENTITY_PROFILE_DOMAINS_ID} className="sidebar-domain-section">
            <SidebarSection
                title="Domain"
                content={
                    <Content>
                        {domain && (
                            <DomainLinkWrapper>
                                <DomainLink
                                    domain={domain}
                                    closable={!readOnly && !updateOnly}
                                    readOnly={readOnly}
                                    onClose={(e) => {
                                        e.preventDefault();
                                        onRemoveDomain(entityData?.domain?.associatedUrn);
                                    }}
                                    fontSize={12}
                                />
                            </DomainLinkWrapper>
                        )}
                        {proposedDomainRequests.map((request) => {
                            const proposedDomain = request.params?.domainProposal?.domain;
                            const displayName = entityRegistry.getDisplayName(EntityType.Domain, proposedDomain);

                            return (
                                <>
                                    {proposedDomain && (
                                        <ProposedDomain
                                            onClick={() => {
                                                setSelectedActionRequest(request);
                                            }}
                                        >
                                            <DomainContent
                                                domain={proposedDomain}
                                                name={displayName}
                                                closable={false}
                                                iconSize={24}
                                            />
                                            <ProposedIcon propertyName="Domain" />
                                        </ProposedDomain>
                                    )}
                                </>
                            );
                        })}

                        {(!domain || !!updateOnly) && (
                            <>{!domain && <EmptySectionText message={EMPTY_MESSAGES.domain.title} />}</>
                        )}
                    </Content>
                }
                extra={
                    <SectionActionButton
                        button={domain ? <EditOutlinedIcon /> : <AddRoundedIcon />}
                        onClick={(event) => {
                            setShowModal(true);
                            event.stopPropagation();
                        }}
                        actionPrivilege={canEditDomains || canProposeDomains}
                    />
                }
            />
            {showModal && (
                <SetDomainModal
                    urns={[urn]}
                    refetch={refetch}
                    onCloseModal={() => {
                        setShowModal(false);
                    }}
                    canEdit={canEditDomains}
                    canPropose={canProposeDomains}
                />
            )}
            {selectedActionRequest && (
                <ProposalModal
                    actionRequest={selectedActionRequest}
                    selectedActionRequest={selectedActionRequest}
                    setSelectedActionRequest={setSelectedActionRequest}
                />
            )}
        </div>
    );
};
