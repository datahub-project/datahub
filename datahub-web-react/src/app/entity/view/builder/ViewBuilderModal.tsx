import React, { useEffect, useState } from 'react';
import styled from 'styled-components';
import { Button, Modal, Typography } from 'antd';
import { DEFAULT_BUILDER_STATE, ViewBuilderState } from '../types';
import { ViewBuilderForm } from './ViewBuilderForm';
import ClickOutside from '../../../shared/ClickOutside';

const modalWidth = 700;
const modalStyle = { top: 40 };
const modalBodyStyle = { paddingRight: 60, paddingLeft: 60, paddingBottom: 40 };

const TitleContainer = styled.div`
    display: flex;
    justify-content: space-between;
`;

const SaveButtonContainer = styled.div`
    width: 100%;
    display: flex;
    justify-content: right;
`;

const CancelButton = styled(Button)`
    margin-right: 12px;
`;

type Props = {
    initialState?: ViewBuilderState;
    onSubmit: (input: ViewBuilderState) => void;
    onCancel?: () => void;
};

export const ViewBuilderModal = ({ initialState, onSubmit, onCancel }: Props) => {
    const [viewBuilderState, setViewBuilderState] = useState<ViewBuilderState>(initialState || DEFAULT_BUILDER_STATE);

    useEffect(() => {
        setViewBuilderState(initialState || DEFAULT_BUILDER_STATE);
    }, [initialState]);

    const modalClosePopup = () => {
        Modal.confirm({
            title: 'Exit View Editor',
            content: `Are you sure you want to exit View editor? All changes will be lost`,
            onOk() {
                onCancel?.();
            },
            onCancel() {},
            okText: 'Yes',
            maskClosable: true,
            closable: true,
        });
    };

    const canSave = viewBuilderState.name && viewBuilderState.viewType && viewBuilderState?.definition?.filter;
    const isEditing = initialState !== undefined;
    const titleText = isEditing ? 'Edit View' : 'New View';

    return (
        <ClickOutside onClickOutside={modalClosePopup} wrapperClassName="test-builder-modal">
            <Modal
                wrapClassName="view-builder-modal"
                footer={null}
                title={
                    <TitleContainer>
                        <Typography.Text>{titleText}</Typography.Text>
                    </TitleContainer>
                }
                style={modalStyle}
                bodyStyle={modalBodyStyle}
                visible
                width={modalWidth}
                onCancel={onCancel}
            >
                <ViewBuilderForm state={viewBuilderState} updateState={setViewBuilderState} />
                <SaveButtonContainer>
                    <CancelButton data-testid="view-builder-cancel" onClick={onCancel}>
                        Cancel
                    </CancelButton>
                    <Button
                        data-testid="view-builder-save"
                        type="primary"
                        disabled={!canSave}
                        onClick={() => onSubmit(viewBuilderState)}
                    >
                        Save
                    </Button>
                </SaveButtonContainer>
            </Modal>
        </ClickOutside>
    );
};
