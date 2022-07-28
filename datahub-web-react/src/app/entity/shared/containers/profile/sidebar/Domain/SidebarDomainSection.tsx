import { Typography, Button, Modal, message } from 'antd';
import React, { useState } from 'react';
import { EditOutlined } from '@ant-design/icons';
import { EMPTY_MESSAGES } from '../../../../constants';
import { useEntityData, useRefetch } from '../../../../EntityContext';
import { SidebarHeader } from '../SidebarHeader';
import { SetDomainModal } from './SetDomainModal';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { EntityType } from '../../../../../../../types.generated';
import { useUnsetDomainMutation } from '../../../../../../../graphql/mutations.generated';
import { DomainLink } from '../../../../../../shared/tags/DomainLink';

export const SidebarDomainSection = () => {
    const { entityData } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const refetch = useRefetch();
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
            <SidebarHeader title="Domain" />
            <div>
                {domain && (
                    <DomainLink
                        urn={domain?.urn}
                        name={entityRegistry.getDisplayName(EntityType.Domain, domain)}
                        closable
                        onClose={(e) => {
                            e.preventDefault();
                            onRemoveDomain(entityData?.domain?.associatedUrn);
                        }}
                    />
                )}
                {!domain && (
                    <>
                        <Typography.Paragraph type="secondary">
                            {EMPTY_MESSAGES.domain.title}. {EMPTY_MESSAGES.domain.description}
                        </Typography.Paragraph>
                        <Button type="default" onClick={() => setShowModal(true)}>
                            <EditOutlined /> Set Domain
                        </Button>
                    </>
                )}
            </div>
            {showModal && (
                <SetDomainModal
                    refetch={refetch}
                    onCloseModal={() => {
                        setShowModal(false);
                    }}
                />
            )}
        </div>
    );
};
