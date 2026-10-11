import { toast } from '@components';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Plus } from '@phosphor-icons/react/dist/csr/Plus';
import { Modal } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { EMPTY_MESSAGES } from '@app/entityV2/shared/constants';
import { SetApplicationModal } from '@app/entityV2/shared/containers/profile/sidebar/Applications/SetApplicationModal';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import SectionActionButton from '@app/entityV2/shared/containers/profile/sidebar/SectionActionButton';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { ApplicationLink } from '@app/shared/tags/ApplicationLink';
import { useAppConfig } from '@app/useAppConfig';

import { useBatchUnsetApplicationMutation } from '@graphql/application.generated';
import { ApplicationAssociation } from '@types';

const Content = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    flex-wrap: wrap;
    text-wrap: wrap;
`;

const ApplicationLinkWrapper = styled.div`
    margin-right: 4px;
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

export const SidebarApplicationSection = ({ readOnly, properties }: Props) => {
    const { t } = useTranslation('entity.shared.containers');
    const { t: tc } = useTranslation('common.actions');
    const {
        config: { visualConfig },
    } = useAppConfig();
    const updateOnly = properties?.updateOnly;
    const { entityData } = useEntityData();
    const refetch = useRefetch();
    const urn = useMutationUrn();
    const [batchUnsetApplicationMutation] = useBatchUnsetApplicationMutation();
    const [showModal, setShowModal] = useState(false);
    const applications = entityData?.applications || [];
    const canEditApplication = !!entityData?.privileges?.canEditProperties;

    if (applications.length === 0 && !visualConfig.application?.showSidebarSectionWhenEmpty) {
        return null;
    }

    const removeApplication = (applicationUrn: string) => {
        batchUnsetApplicationMutation({
            variables: {
                input: {
                    applicationUrn,
                    resourceUrns: [urn],
                },
            },
        })
            .then(() => {
                toast.success(t('sidebar.application.removeSuccess'), { duration: 2 });
                refetch?.();
            })
            .catch((e: unknown) => {
                toast.destroy();
                if (e instanceof Error) {
                    toast.error(t('sidebar.application.removeFailed', { message: e.message || '' }), { duration: 3 });
                }
            });
    };

    const onRemoveApplication = (applicationUrn: string) => {
        Modal.confirm({
            title: t('sidebar.application.removeConfirmTitle'),
            content: t('sidebar.application.removeConfirmContent'),
            onOk() {
                removeApplication(applicationUrn);
            },
            onCancel() {},
            okText: tc('yes'),
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <div className="sidebar-application-section">
            <SidebarSection
                title={t('sidebar.application.sectionTitle')}
                content={
                    <Content>
                        {applications.length > 0 &&
                            applications.map((appAssociation: ApplicationAssociation) => (
                                <ApplicationLinkWrapper key={appAssociation.application?.urn}>
                                    <ApplicationLink
                                        application={appAssociation.application}
                                        closable={!readOnly && !updateOnly && canEditApplication}
                                        readOnly={readOnly}
                                        onClose={(e) => {
                                            e.preventDefault();
                                            if (appAssociation.application?.urn) {
                                                onRemoveApplication(appAssociation.application.urn);
                                            }
                                        }}
                                        fontSize={12}
                                    />
                                </ApplicationLinkWrapper>
                            ))}
                        {(applications.length === 0 || !!updateOnly) && (
                            <>
                                {applications.length === 0 && (
                                    <EmptySectionText message={EMPTY_MESSAGES.application.title} />
                                )}
                            </>
                        )}
                    </Content>
                }
                extra={
                    !readOnly && (
                        <SectionActionButton
                            dataTestId="add-applications-button"
                            icon={applications.length > 0 ? PencilSimple : Plus}
                            onClick={(event) => {
                                setShowModal(true);
                                event.stopPropagation();
                            }}
                            actionPrivilege={canEditApplication}
                        />
                    )
                }
            />
            {showModal && (
                <SetApplicationModal
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
