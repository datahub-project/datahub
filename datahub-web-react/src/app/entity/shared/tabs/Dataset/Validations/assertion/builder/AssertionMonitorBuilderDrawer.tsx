import { Drawer, Modal, Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';

import { AssertionMonitorBuilder } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/AssertionMonitorBuilder';
import { AssertionMonitorBuilderState } from '@app/entity/shared/tabs/Dataset/Validations/assertion/builder/types';
import ClickOutside from '@app/shared/ClickOutside';

import { Assertion, EntityType } from '@types';

const modalStyle = {};
const modalBodyStyle = { paddingRight: 48, paddingLeft: 48, paddingBottom: 20 };

const TitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

type Props = {
    entityUrn: string;
    entityType: EntityType;
    platformUrn: string;
    initialState?: AssertionMonitorBuilderState;
    onSubmit?: (assertion: Assertion) => void;
    onCancel?: () => void;
};

/**
 * This component is a modal used for constructing Assertion Monitors,
 * which are responsible for periodically evaluating assertions.
 */
export const AssertionMonitorBuilderDrawer = ({
    entityUrn,
    entityType,
    platformUrn,
    initialState,
    onSubmit,
    onCancel,
}: Props) => {
    const isEditing = initialState !== undefined;
    const titleText = isEditing ? 'Edit Assertion Monitor' : 'New Assertion Monitor';

    const modalClosePopup = () => {
        Modal.confirm({
            title: 'Exit Editor',
            content: `Are you sure you want to exit the editor? All changes will be lost`,
            onOk() {
                onCancel?.();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    return (
        <ClickOutside onClickOutside={modalClosePopup} wrapperClassName="assertion-monitor-builder-modal">
            <Drawer
                className="assertion-monitor-builder-modal"
                footer={null}
                title={
                    <TitleContainer>
                        <Typography.Text>{titleText}</Typography.Text>
                    </TitleContainer>
                }
                style={modalStyle}
                bodyStyle={modalBodyStyle}
                visible
                onClose={modalClosePopup}
                width={600}
            >
                <AssertionMonitorBuilder
                    entityUrn={entityUrn}
                    entityType={entityType}
                    platformUrn={platformUrn}
                    initialState={initialState}
                    onSubmit={onSubmit}
                    onCancel={onCancel}
                />
            </Drawer>
        </ClickOutside>
    );
};
