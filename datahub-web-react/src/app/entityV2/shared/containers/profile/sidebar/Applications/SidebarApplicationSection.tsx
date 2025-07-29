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

import { useBatchSetApplicationMutation } from '@graphql/application.generated';

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
    const [batchSetApplicationMutation] = useBatchSetApplicationMutation();
    const [showModal, setShowModal] = useState(false);
    const application = entityData?.application?.application;
    const canEditApplication = !!entityData?.privileges?.canEditProperties;

    if (!application && !visualConfig.application?.showSidebarSectionWhenEmpty) {
        return null;
    }

    const removeApplication = () => {
        batchSetApplicationMutation({
            variables: {
                input: {
                    applicationUrn: null,
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

    const onRemoveApplication = () => {
        Modal.confirm({
            title: `Confirm Application Removal`,
            content: `Are you sure you want to remove this application?`,
            onOk() {
                removeApplication();
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
                title="Application"
                content={
                    <Content>
                        {application && (
                            <ApplicationLinkWrapper>
                                <ApplicationLink
                                    application={application}
                                    closable={!readOnly && !updateOnly && canEditApplication}
                                    readOnly={readOnly}
                                    onClose={(e) => {
                                        e.preventDefault();
                                        onRemoveApplication();
                                    }}
                                    fontSize={12}
                                />
                            </ApplicationLinkWrapper>
                        )}
                        {(!application || !!updateOnly) && (
                            <>{!application && <EmptySectionText message={EMPTY_MESSAGES.application.title} />}</>
                        )}
                    </Content>
                }
                extra={
                    !readOnly && (
                        <SectionActionButton
                            dataTestId="add-applications-button"
                            button={application ? <EditOutlinedIcon /> : <AddRoundedIcon />}
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
