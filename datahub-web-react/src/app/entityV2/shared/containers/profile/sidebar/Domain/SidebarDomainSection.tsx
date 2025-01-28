import React, { useState } from 'react';
import styled from 'styled-components';
import { Modal, message } from 'antd';
import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { EMPTY_MESSAGES } from '../../../../constants';
import { useEntityData, useMutationUrn, useRefetch } from '../../../../../../entity/shared/EntityContext';
import { SetDomainModal } from './SetDomainModal';
import { useUnsetDomainMutation } from '../../../../../../../graphql/mutations.generated';
import { DomainLink } from '../../../../../../sharedV2/tags/DomainLink';
import { ENTITY_PROFILE_DOMAINS_ID } from '../../../../../../onboarding/config/EntityProfileOnboardingConfig';
import { SidebarSection } from '../SidebarSection';
import SectionActionButton from '../SectionActionButton';
import EmptySectionText from '../EmptySectionText';

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
    const { entityData } = useEntityData();
    const refetch = useRefetch();
    const urn = useMutationUrn();
    const [unsetDomainMutation] = useUnsetDomainMutation();
    const [showModal, setShowModal] = useState(false);
    const domain = entityData?.domain?.domain;

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
