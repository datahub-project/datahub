import { EditOutlined } from '@ant-design/icons';
import { Button, Modal, Typography, message } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityData, useMutationUrn, useRefetch } from '@app/entity/shared/EntityContext';
import { EMPTY_MESSAGES } from '@app/entity/shared/constants';
import { SidebarHeader } from '@app/entity/shared/containers/profile/sidebar/SidebarHeader';
import { ENTITY_PROFILE_APPLICATIONS_ID } from '@app/onboarding/config/EntityProfileOnboardingConfig';

import { useBatchSetApplicationMutation } from '@graphql/application.generated';

import { ApplicationLink } from '../../../../../../shared/tags/ApplicationLink';
import { SetApplicationModal } from './SetApplicationModal';

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

export const SidebarApplicationSection = ({ readOnly, properties }: Props) => {
    const updateOnly = properties?.updateOnly;
    const { entityData } = useEntityData();
    const refetch = useRefetch();
    const urn = useMutationUrn();
    const [batchSetApplicationMutation] = useBatchSetApplicationMutation();
    const [showModal, setShowModal] = useState(false);
    const application = entityData?.application;

    console.log('application', application);

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
        <div>
            <div id={ENTITY_PROFILE_APPLICATIONS_ID} className="sidebar-application-section">
                <SidebarHeader title="Application" />
                <ContentWrapper displayInline={!!application}>
                    {application && (
                        <ApplicationLink
                            application={application}
                            closable={!readOnly && !updateOnly}
                            readOnly={readOnly}
                            onClose={(e) => {
                                e.preventDefault();
                                onRemoveApplication();
                            }}
                            fontSize={12}
                        />
                    )}
                    {(!application || !!updateOnly) && (
                        <>
                            {!application && (
                                <Typography.Paragraph type="secondary">
                                    {EMPTY_MESSAGES.application.title}. {EMPTY_MESSAGES.application.description}
                                </Typography.Paragraph>
                            )}
                            {!readOnly && (
                                <StyledButton type="default" onClick={() => setShowModal(true)}>
                                    <EditOutlined /> Set Application
                                </StyledButton>
                            )}
                        </>
                    )}
                </ContentWrapper>
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
        </div>
    );
};
