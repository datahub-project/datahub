import { Drawer, Modal } from 'antd';
import React, { useState } from 'react';

import { IncidentDrawerHeader } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentDrawerHeader';
import { IncidentEditor } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentEditor';
import { IncidentView } from '@app/entityV2/shared/tabs/Incident/AcrylComponents/IncidentView';
import { IncidentAction } from '@app/entityV2/shared/tabs/Incident/constant';
import { EntityStagedForIncident, IncidentTableRow } from '@app/entityV2/shared/tabs/Incident/types';
import ClickOutside from '@src/app/shared/ClickOutside';
import { EntityPrivileges, Incident } from '@src/types.generated';

const modalBodyStyle = { padding: 0, fontFamily: 'Mulish, sans-serif' };

type IncidentDetailDrawerProps = {
    entity: EntityStagedForIncident;
    mode: IncidentAction;
    incident?: IncidentTableRow;
    onCancel?: () => void;
    onSubmit?: (incident?: Incident) => void;
    privileges?: EntityPrivileges;
};

export const IncidentDetailDrawer = ({
    entity,
    mode,
    onCancel,
    onSubmit,
    incident,
    privileges,
}: IncidentDetailDrawerProps) => {
    const [isEditView, setIsEditView] = useState<boolean>(false);
    const showEditor = isEditView || mode === IncidentAction.CREATE;

    const onCloseModal = () => {
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

    const handleSubmit = (i?: Incident) => {
        setIsEditView(false);
        onSubmit?.(i);
    };

    return (
        <ClickOutside onClickOutside={onCloseModal} wrapperClassName="incident-monitor-builder-modal">
            <Drawer
                width={600}
                placement="right"
                closable={false}
                visible
                bodyStyle={modalBodyStyle}
                onClose={onCloseModal}
            >
                <IncidentDrawerHeader
                    mode={mode}
                    onClose={onCancel}
                    isEditActive={isEditView}
                    setIsEditActive={setIsEditView}
                    data={incident}
                    platform={entity?.platform}
                    privileges={privileges}
                />
                {showEditor ? (
                    <IncidentEditor
                        onClose={onCancel}
                        data={incident}
                        mode={mode}
                        incidentUrn={incident?.urn}
                        entity={entity}
                        onSubmit={handleSubmit}
                    />
                ) : (
                    <IncidentView incident={incident as IncidentTableRow} />
                )}
            </Drawer>
        </ClickOutside>
    );
};
