import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';
import { SetDomainModal } from '@app/entityV2/shared/containers/profile/sidebar/Domain/SetDomainModal';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { ENTITY_PROFILE_DOMAINS_ID } from '@app/onboarding/config/EntityProfileOnboardingConfig';
import { ConfirmationModal } from '@app/sharedV2/modals/ConfirmationModal';
import { useReloadableContext } from '@app/sharedV2/reloadableContext/hooks/useReloadableContext';
import { ReloadableKeyTypeNamespace } from '@app/sharedV2/reloadableContext/types';
import { getReloadableKeyType } from '@app/sharedV2/reloadableContext/utils';
import { DomainLink } from '@app/sharedV2/tags/DomainLink';

import { useUnsetDomainMutation } from '@graphql/mutations.generated';
import { DataHubPageModuleType, EntityType } from '@types';

const Content = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    flex-wrap: wrap;
    text-wrap: wrap;
`;

const DomainLinkWrapper = styled.div`
    margin-right: 12px;
    display: flex;
    align-items: center;
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
    const { entityData, entityType } = useEntityData();
    const refetch = useRefetch();
    const urn = useMutationUrn();
    const [unsetDomainMutation] = useUnsetDomainMutation();
    const [showModal, setShowModal] = useState(false);
    const [domainToRemove, setDomainToRemove] = useState<string | undefined>();
    const domain = entityData?.domain?.domain;

    const { reloadByKeyType } = useReloadableContext();

    const canEditDomains = !!entityData?.privileges?.canEditDomains;

    const removeDomain = (urnToRemoveFrom) => {
        unsetDomainMutation({ variables: { entityUrn: urnToRemoveFrom } })
            .then(() => {
                message.success({ content: 'Removed Domain.', duration: 2 });
                refetch?.();
                // Reload modules
                // Assets - as assets module in domain summary tab could be updated
                reloadByKeyType(
                    [getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.Assets)],
                    3000,
                );
                // DataProduct - as data products module in domain summary tab could be updated
                if (entityType === EntityType.DataProduct) {
                    reloadByKeyType(
                        [getReloadableKeyType(ReloadableKeyTypeNamespace.MODULE, DataHubPageModuleType.DataProducts)],
                        3000,
                    );
                }
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to remove domain: \n ${e.message || ''}`, duration: 3 });
                }
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
                                        setDomainToRemove(entityData?.domain?.associatedUrn);
                                    }}
                                    fontSize={12}
                                />
                            </DomainLinkWrapper>
                        )}
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
                        dataTestId="set-domain-button"
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
            <ConfirmationModal
                isOpen={!!domainToRemove}
                handleClose={() => setDomainToRemove(undefined)}
                handleConfirm={() => {
                    removeDomain(domainToRemove);
                    setDomainToRemove(undefined);
                }}
                modalTitle="Confirm Domain Removal"
                modalText="Are you sure you want to remove this domain?"
            />
        </div>
    );
};
