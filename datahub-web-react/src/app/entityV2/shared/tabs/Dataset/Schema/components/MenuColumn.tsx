import { CopyOutlined } from '@ant-design/icons';
import { PencilSimple } from '@phosphor-icons/react/dist/csr/PencilSimple';
import { Prohibit } from '@phosphor-icons/react/dist/csr/Prohibit';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import { Dropdown, Menu } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { VscGraphLeft } from 'react-icons/vsc';
import styled from 'styled-components/macro';

import { useUserContext } from '@app/context/useUserContext';
import { useEntityData, useMutationUrn, useRefetch, useRouteToTab } from '@app/entity/shared/EntityContext';
import { MenuIcon } from '@app/entityV2/shared/EntityDropdown/EntityMenuActions';
import { useUndeprecateResource } from '@app/entityV2/shared/EntityDropdown/useUndeprecateResource';
import { canShowEditDeprecation } from '@app/entityV2/shared/EntityDropdown/utils';
import DeleteLogicalModelColumnButton from '@app/entityV2/shared/logicalModels/DeleteLogicalModelColumnButton';
import EditLogicalModelColumnModal from '@app/entityV2/shared/logicalModels/EditLogicalModelColumnModal';
import { isLogicalModel } from '@app/entityV2/shared/logicalModels/logicalModels.utils';
import { useSchemaRefetch } from '@app/entityV2/shared/tabs/Dataset/Schema/SchemaContext';
import { generateSchemaFieldUrn } from '@app/entityV2/shared/tabs/Lineage/utils';
import { useAppConfig } from '@app/useAppConfig';

import { SchemaField, SchemaFieldDataType, SubResourceType } from '@types';

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
    const { t: te } = useTranslation('entity.shared.entityDropdown');
    const routeToTab = useRouteToTab();
    const { urn, entityType, entityData } = useEntityData();
    const mutationUrn = useMutationUrn();
    const refetch = useRefetch();
    // A column's deprecation is carried by the schema query, which the entity refetch doesn't cover,
    // so both are refreshed — the same pairing the other column mutations use.
    const schemaRefetch = useSchemaRefetch();
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

    const isDeprecated = !!field.schemaFieldEntity?.deprecation?.deprecated;
    // A column's deprecation is authorized against its parent asset, the same privilege the mutation
    // itself checks, so users who lack it aren't offered the action.
    const canUndeprecate = canShowEditDeprecation(entityData?.privileges);
    const undeprecateField = useUndeprecateResource({
        // The deprecation was written against the mutation urn — the primary of a sibling pair, not
        // whichever side is being viewed — so the clear has to address that same urn, as the deprecate
        // flow in FieldDetails does.
        urn: mutationUrn,
        subResource: field.fieldPath,
        subResourceType: SubResourceType.DatasetField,
        refetch: () => Promise.all([refetch?.(), schemaRefetch?.()]),
    });

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
                        {isDeprecated && canUndeprecate && (
                            <Menu.Item key="5" onClick={(e) => e.domEvent.stopPropagation()}>
                                <MenuItem onClick={() => undeprecateField()} data-testid="un-deprecate-column">
                                    <Prohibit size={16} /> &nbsp; {te('deprecation.markUnDeprecated')}
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
