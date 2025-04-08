import { EditOutlined } from '@ant-design/icons';
import { Button, Modal, Typography, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { EMPTY_MESSAGES } from '@app/entity/shared/constants';
import { SetDomainModal } from '@app/entity/shared/containers/profile/sidebar/Domain/SetDomainModal';
import { SidebarHeader } from '@app/entity/shared/containers/profile/sidebar/SidebarHeader';
import { ENTITY_PROFILE_DOMAINS_ID } from '@app/onboarding/config/EntityProfileOnboardingConfig';
import { DomainLink } from '@app/shared/tags/DomainLink';

import { useUnsetDomainMutation } from '@graphql/mutations.generated';

const StyledButton = styled(Button)`
    display: block;
    margin-bottom: 8px;
`;

const ContentWrapper = styled.div<{ displayInline: boolean }>`
    ${(props) =>
        props.displayInline &&
        `
    display: flex;
    align-items: center;
    `}
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
        <div>
            <div id={ENTITY_PROFILE_DOMAINS_ID} className="sidebar-domain-section">
                <SidebarHeader title="Domain" />
                <ContentWrapper displayInline={!!domain}>
                    {domain && (
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
                    )}
                    {(!domain || !!updateOnly) && (
                        <>
                            {!domain && (
                                <Typography.Paragraph type="secondary">
                                    {EMPTY_MESSAGES.domain.title}. {EMPTY_MESSAGES.domain.description}
                                </Typography.Paragraph>
                            )}
                            {!readOnly && (
                                <StyledButton type="default" onClick={() => setShowModal(true)}>
                                    <EditOutlined /> Set Domain
                                </StyledButton>
                            )}
                        </>
                    )}
                </ContentWrapper>
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
        </div>
    );
};
