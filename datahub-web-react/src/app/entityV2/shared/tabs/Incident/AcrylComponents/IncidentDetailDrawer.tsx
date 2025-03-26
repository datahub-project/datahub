import React, { useState } from 'react';

import { Drawer, Modal } from 'antd';
import ClickOutside from '@src/app/shared/ClickOutside';
import { Incident } from '@src/types.generated';
import { IncidentDrawerHeader } from './IncidentDrawerHeader';
import { IncidentView } from './IncidentView';
import { IncidentEditor } from './IncidentEditor';
import { IncidentTableRow } from '../types';
import { IncidentAction } from '../constant';

const modalBodyStyle = { padding: 0, fontFamily: 'Mulish, sans-serif' };

type IncidentDetailDrawerProps = {
    urn: string;
    mode: IncidentAction;
    incident?: IncidentTableRow;
    onCancel?: () => void;
    onSubmit?: (incident?: Incident) => void;
};

export const IncidentDetailDrawer = ({ mode, onCancel, onSubmit, incident }: IncidentDetailDrawerProps) => {
    const [isEditView, setIsEditView] = useState<boolean>(false);
    const showEditor = isEditView || mode === IncidentAction.CREATE;
    const modalClosePopup = () => {
        if (showEditor) {
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
        } else {
            onCancel?.();
        }
    };
    return (
        <ClickOutside onClickOutside={modalClosePopup} wrapperClassName="incident-monitor-builder-modal">
            <Drawer
                width={600}
                placement="right"
                closable={false}
                visible
                bodyStyle={modalBodyStyle}
                onClose={modalClosePopup}
            >
                <IncidentDrawerHeader
                    mode={mode}
                    onClose={onCancel}
                    isEditActive={isEditView}
                    setIsEditActive={setIsEditView}
                    data={incident}
                />
                {showEditor ? (
                    <IncidentEditor
                        onClose={onCancel}
                        data={incident}
                        mode={mode}
                        incidentUrn={incident?.urn}
                        onSubmit={onSubmit}
                    />
                ) : (
                    <IncidentView incident={incident as IncidentTableRow} />
                )}
            </Drawer>
        </ClickOutside>
    );
};
