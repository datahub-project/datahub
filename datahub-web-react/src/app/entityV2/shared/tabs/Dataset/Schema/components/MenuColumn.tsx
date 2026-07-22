import { CopyOutlined } from '@ant-design/icons';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import { Dropdown, Menu } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { VscGraphLeft } from 'react-icons/vsc';
import styled from 'styled-components/macro';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData, useRefetch, useRouteToTab } from '@app/entity/shared/EntityContext';
import { MenuIcon } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import DeleteLogicalModelColumnButton from '@app/entityV2/shared/logicalModels/DeleteLogicalModelColumnButton';
import EditLogicalModelColumnModal from '@app/entityV2/shared/logicalModels/EditLogicalModelColumnModal';
import { isLogicalModel } from '@app/entityV2/shared/logicalModels/logicalModels.utils';
import { generateSchemaFieldUrn } from '@app/entityV2/shared/tabs/Lineage/utils';
import { useAppConfig } from '@app/useAppConfig';

import { SchemaField, SchemaFieldDataType } from '@types';

const LINEAGE_TAB = 'Lineage';

export const ImpactAnalysisIcon = styled(VscGraphLeft)`
    transform: scaleX(-1);
    font-size: 18px;
`;

const CopyOutlinedIcon = styled(CopyOutlined)`
    transform: scaleX(-1);
    font-size: 16px;
`;

const MenuItem = styled.div`
    align-items: center;
    display: flex;
    font-size: 12px;
    padding: 0 4px;
    color: ${(props) => props.theme.colors.text};
`;

interface Props {
    field: SchemaField;
}

export default function MenuColumn({ field }: Props) {
    const { t } = useTranslation('entity.profile.schema');
    const { t: tl } = useTranslation('logicalModels');
    const routeToTab = useRouteToTab();
    const { urn, entityType, entityData } = useEntityData();
    const refetch = useRefetch();
    const selectedColumnUrn = generateSchemaFieldUrn(field.fieldPath, urn);
    const { logicalModelsEnabled } = useAppConfig().config.featureFlags;
    const { platformPrivileges } = useUserContext();
    // Editing columns runs through updateLogicalModelSchema, which requires the
    // CREATE_LOGICAL_MODELS platform privilege — hide the actions from users who lack it.
    const showLogicalActions =
        logicalModelsEnabled && isLogicalModel(entityType, entityData) && !!platformPrivileges?.createLogicalModels;
    const childCount = entityData?.physicalChildren?.total ?? 0;
    const [editOpen, setEditOpen] = useState(false);
    const [deleteOpen, setDeleteOpen] = useState(false);

    return (
        <>
            <Dropdown
                overlay={
                    <Menu>
                        <Menu.Item key="0" onClick={(e) => e.domEvent.stopPropagation()}>
                            <MenuItem
                                onClick={() =>
                                    routeToTab({ tabName: LINEAGE_TAB, tabParams: { column: field.fieldPath } })
                                }
                            >
                                <ImpactAnalysisIcon /> &nbsp; {t('menuColumn.seeColumnLineage')}
                            </MenuItem>
                        </Menu.Item>
                        {navigator.clipboard && (
                            <Menu.Item key="1" onClick={(e) => e.domEvent.stopPropagation()}>
                                <MenuItem onClick={() => navigator.clipboard.writeText(field.fieldPath)}>
                                    <CopyOutlinedIcon /> &nbsp; {t('menuColumn.copyColumnFieldPath')}
                                </MenuItem>
                            </Menu.Item>
                        )}
                        {navigator.clipboard && (
                            <Menu.Item key="2" onClick={(e) => e.domEvent.stopPropagation()}>
                                <MenuItem onClick={() => navigator.clipboard.writeText(selectedColumnUrn || '')}>
                                    <CopyOutlinedIcon /> &nbsp; {t('menuColumn.copyColumnUrn')}
                                </MenuItem>
                            </Menu.Item>
                        )}
                        {showLogicalActions && (
                            <Menu.Item key="3" onClick={(e) => e.domEvent.stopPropagation()}>
                                <MenuItem onClick={() => setEditOpen(true)} data-testid="edit-logical-model-column">
                                    <PencilSimple size={16} /> &nbsp; {tl('editColumn.menuLabel')}
                                </MenuItem>
                            </Menu.Item>
                        )}
                        {showLogicalActions && (
                            <Menu.Item key="4" onClick={(e) => e.domEvent.stopPropagation()}>
                                <MenuItem onClick={() => setDeleteOpen(true)} data-testid="delete-logical-model-column">
                                    <Trash size={16} /> &nbsp; {tl('deleteColumn.menuLabel')}
                                </MenuItem>
                            </Menu.Item>
                        )}
                    </Menu>
                }
                trigger={['click']}
            >
                <MenuIcon fontSize={16} onClick={(e) => e.stopPropagation()} />
            </Dropdown>
            {editOpen && (
                <EditLogicalModelColumnModal
                    datasetUrn={urn}
                    fieldPath={field.fieldPath}
                    currentType={(field.type as SchemaFieldDataType) ?? SchemaFieldDataType.String}
                    childCount={childCount}
                    onClose={() => setEditOpen(false)}
                    onUpdated={refetch}
                />
            )}
            <DeleteLogicalModelColumnButton
                datasetUrn={urn}
                fieldPath={field.fieldPath}
                childCount={childCount}
                open={deleteOpen}
                onClose={() => setDeleteOpen(false)}
                onDeleted={refetch}
            />
        </>
    );
}
