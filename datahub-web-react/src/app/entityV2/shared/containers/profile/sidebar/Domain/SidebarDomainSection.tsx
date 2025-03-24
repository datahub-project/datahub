import React, { useState } from 'react';
import styled from 'styled-components';
import { Modal, message } from 'antd';
import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { getProposedItemsByType } from '@src/app/entityV2/shared/utils';
import { ActionRequestType, EntityType } from '@src/types.generated';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { colors } from '@src/alchemy-components';
import { EMPTY_MESSAGES } from '../../../../constants';
import { useEntityData, useMutationUrn, useRefetch } from '../../../../../../entity/shared/EntityContext';
import { SetDomainModal } from './SetDomainModal';
import { useUnsetDomainMutation } from '../../../../../../../graphql/mutations.generated';
import { DomainContent, DomainLink } from '../../../../../../sharedV2/tags/DomainLink';
import { ENTITY_PROFILE_DOMAINS_ID } from '../../../../../../onboarding/config/EntityProfileOnboardingConfig';
import { SidebarSection } from '../SidebarSection';
import SectionActionButton from '../SectionActionButton';
import EmptySectionText from '../EmptySectionText';

const Content = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    flex-wrap: wrap;
    text-wrap: wrap;
`;

const DomainLinkWrapper = styled.div`
    margin-right: 12px;
    margin-bottom: 4px;
    display: flex;
    align-items: center;
`;

const ProposedDomain = styled.div`
    margin-right: 12px;
    margin-bottom: 4px;
    display: flex;
    align-items: center;
    padding: 3px;
    border-radius: 8px;
    border: 1px dashed ${colors.gray[200]};
    color: ${colors.gray[500]};
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

    const proposedDomainRequests = getProposedItemsByType(
        entityData?.proposals || [],
        ActionRequestType.DomainAssociation,
    );
    const proposedDomains = proposedDomainRequests.flatMap((request) => request.params?.domainProposal?.domain || []);
    const canEditDomains = !!entityData?.privileges?.canEditDomains;

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
                        {proposedDomains.map((proposedDomain) => {
                            const displayName = entityRegistry.getDisplayName(EntityType.Domain, proposedDomain);
                            return (
                                <ProposedDomain>
                                    <DomainContent
                                        domain={proposedDomain}
                                        name={displayName}
                                        closable={false}
                                        iconSize={24}
                                    />
                                </ProposedDomain>
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
                        actionPrivilege={canEditDomains}
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
                />
            )}
        </div>
    );
};
