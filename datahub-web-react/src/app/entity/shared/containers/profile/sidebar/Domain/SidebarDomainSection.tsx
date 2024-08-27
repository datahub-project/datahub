import { Typography, Button, Modal, message } from 'antd';
import React, { useState } from 'react';
import { EditOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { useTranslation } from 'react-i18next';
import { useEntityData, useMutationUrn, useRefetch } from '../../../../EntityContext';
import { SidebarHeader } from '../SidebarHeader';
import { SetDomainModal } from './SetDomainModal';
import { useUnsetDomainMutation } from '../../../../../../../graphql/mutations.generated';
import { DomainLink } from '../../../../../../shared/tags/DomainLink';
import { ENTITY_PROFILE_DOMAINS_ID } from '../../../../../../onboarding/config/EntityProfileOnboardingConfig';
import { translateDisplayNames } from '../../../../../../../utils/translation/translation';

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
    const { t } = useTranslation();
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
                message.success({ content: t('common.removed'), duration: 2 });
                refetch?.();
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `${t('crud.error.remove')}\n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    const onRemoveDomain = (urnToRemoveFrom) => {
        Modal.confirm({
            title: t('domain.confirmRemovedDomainTitle'),
            content: t('domain.confirmRemovedDomain'),
            onOk() {
                removeDomain(urnToRemoveFrom);
            },
            onCancel() {},
            okText: t('common.yes'),
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <div>
            <div id={ENTITY_PROFILE_DOMAINS_ID} className="sidebar-domain-section">
                <SidebarHeader title={t('common.domain')} />
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
                                    {translateDisplayNames(t, 'emptyTitleDomain')}.{' '}
                                    {translateDisplayNames(t, 'emptyDescriptionDomain')}
                                </Typography.Paragraph>
                            )}
                            {!readOnly && (
                                <StyledButton type="default" onClick={() => setShowModal(true)}>
                                    <EditOutlined /> {t('domain.setDomain')}
                                </StyledButton>
                            )}
                        </>
                    )}
                </ContentWrapper>
                {showModal && (
                    <SetDomainModal
                        titleOverride={t('domain.setDomain')}
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
