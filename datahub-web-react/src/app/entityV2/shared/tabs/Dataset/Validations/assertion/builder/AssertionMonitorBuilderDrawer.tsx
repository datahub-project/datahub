import React from 'react';
import ClickOutside from '@src/app/shared/ClickOutside';
import { capitalizeFirstLetter } from '@src/app/shared/textUtil';
import PlatformIcon from '@src/app/sharedV2/icons/PlatformIcon';
import { Assertion, AssertionType, DataPlatform, EntityType } from '@src/types.generated';
import { Drawer, Modal, Typography } from 'antd';
import styled from 'styled-components';
import { AssertionMonitorBuilder } from './AssertionMonitorBuilder';
import { AssertionMonitorBuilderState } from './types';

const modalStyle = {};
const modalBodyStyle = { paddingRight: 48, paddingLeft: 48, paddingBottom: 20 };

const TitleContainer = styled.div`
    display: flex;
    align-items: center;
    // justify-content: space-between;
`;
const ForPlatformWrapper = styled.div`
    display: flex;
    flex-direction: row;
    align-items: center;
    font-size: 0.75rem;
    margin-left: 12px;
    padding-left: 12px;
    border-left: 0.5px solid #ddd;
`;

type Props = {
    entityUrn: string;
    entityType: EntityType;
    platform: DataPlatform;
    initialState?: AssertionMonitorBuilderState;
    onSubmit?: (assertion: Assertion) => void;
    onCancel?: () => void;
    predefinedType?: AssertionType;
};

/**
 * This component is a modal used for constructing Assertion Monitors,
 * which are responsible for periodically evaluating assertions.
 */
export const AssertionMonitorBuilderDrawer = ({
    entityUrn,
    entityType,
    platform,
    initialState,
    onSubmit,
    onCancel,
    predefinedType,
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
                        <ForPlatformWrapper>
                            <PlatformIcon platform={platform} size={16} styles={{ marginRight: 4 }} />
                            {capitalizeFirstLetter(platform.name)}
                        </ForPlatformWrapper>
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
                    platformUrn={platform.urn}
                    initialState={initialState}
                    onSubmit={onSubmit}
                    onCancel={onCancel}
                    predefinedType={predefinedType}
                />
            </Drawer>
        </ClickOutside>
    );
};
