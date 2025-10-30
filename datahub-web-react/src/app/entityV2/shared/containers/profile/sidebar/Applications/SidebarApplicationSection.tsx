import AddRoundedIcon from '@mui/icons-material/AddRounded';
import EditOutlinedIcon from '@mui/icons-material/EditOutlined';
import { Modal, message } from 'antd';
import React, { useState } from 'react';
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

export const SidebarApplicationSection = ({ readOnly, properties }: Props) => {
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
                message.success({ content: 'Removed Application.', duration: 2 });
                refetch?.();
            })
            .catch((e: unknown) => {
                message.destroy();
                if (e instanceof Error) {
                    message.error({ content: `Failed to remove application: \n ${e.message || ''}`, duration: 3 });
                }
            });
    };

    const onRemoveApplication = (applicationUrn: string) => {
        Modal.confirm({
            title: `Confirm Application Removal`,
            content: `Are you sure you want to remove this application?`,
            onOk() {
                removeApplication(applicationUrn);
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <div className="sidebar-application-section">
            <SidebarSection
                title="Applications"
                content={
                    <Content>
                        {applications.length > 0 &&
                            applications.map((appAssociation: ApplicationAssociation) => (
                                <ApplicationLinkWrapper key={appAssociation.application?.urn}>
                                    <ApplicationLink
                                        application={appAssociation.application}
                                        closable={!readOnly && !updateOnly && canEditApplication}
                                        readOnly={readOnly}
                                        onClose={(e: React.MouseEvent) => {
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
                            button={applications.length > 0 ? <EditOutlinedIcon /> : <AddRoundedIcon />}
                            onClick={(event: React.MouseEvent) => {
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
