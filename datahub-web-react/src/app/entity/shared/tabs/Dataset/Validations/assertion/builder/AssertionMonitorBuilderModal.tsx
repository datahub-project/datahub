import React, { useState } from 'react';
import styled from 'styled-components';
import { Button, Modal, Typography } from 'antd';
import { ExpandAltOutlined, ShrinkOutlined } from '@ant-design/icons';
import { AssertionMonitorBuilder } from './AssertionMonitorBuilder';
import { AssertionMonitorBuilderState } from './types';
import { EntityType, Monitor, Assertion } from '../../../../../../../../types.generated';
import ClickOutside from '../../../../../../../shared/ClickOutside';

const modalStyle = {};
const modalBodyStyle = { paddingRight: 48, paddingLeft: 48, paddingBottom: 20 };

const ExpandButton = styled(Button)`
    && {
        margin-right: 32px;
    }
`;

const TitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

type Props = {
    entityUrn: string;
    entityType: EntityType;
    platformUrn: string;
    initialState?: AssertionMonitorBuilderState;
    onSubmit?: (assertion: Assertion, monitor: Monitor) => void;
    onCancel?: () => void;
};

/**
 * This component is a modal used for constructing Assertion Monitors,
 * which are responsible for periodically evaluating assertions.
 */
export const AssertionMonitorBuilderModal = ({
    entityUrn,
    entityType,
    platformUrn,
    initialState,
    onSubmit,
    onCancel,
}: Props) => {
    const isEditing = initialState !== undefined;
    const titleText = isEditing ? 'Edit Assertion Monitor' : 'New Assertion Monitor';

    const [modalExpanded, setModalExpanded] = useState(false);

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
            <Modal
                wrapClassName="assertion-monitor-builder-modal"
                width={modalExpanded ? 1600 : 840}
                footer={null}
                title={
                    <TitleContainer>
                        <Typography.Text>{titleText}</Typography.Text>
                        <ExpandButton onClick={() => setModalExpanded(!modalExpanded)}>
                            {(modalExpanded && <ShrinkOutlined />) || <ExpandAltOutlined />}
                        </ExpandButton>
                    </TitleContainer>
                }
                style={modalStyle}
                bodyStyle={modalBodyStyle}
                visible
                onCancel={onCancel}
            >
                <AssertionMonitorBuilder
                    entityUrn={entityUrn}
                    entityType={entityType}
                    platformUrn={platformUrn}
                    initialState={initialState}
                    onSubmit={onSubmit}
                    onCancel={onCancel}
                />
            </Modal>
        </ClickOutside>
    );
};
