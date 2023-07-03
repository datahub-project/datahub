import React from 'react';
import { VscGraphLeft } from 'react-icons/vsc';
import { CopyOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { Dropdown, Menu } from 'antd';
import { MenuIcon } from '../../../../EntityDropdown/EntityDropdown';
import { useEntityData, useRouteToTab } from '../../../../EntityContext';
import { SchemaField } from '../../../../../../../types.generated';
import { generateSchemaFieldUrn } from '../../../Lineage/utils';

export const ImpactAnalysisIcon = styled(VscGraphLeft)`
    transform: scaleX(-1);
    font-size: 18px;
`;

export const CopyOutlinedIcon = styled(CopyOutlined)`
    transform: scaleX(-1);
    font-size: 16px;
`;

const MenuItem = styled.div`
    align-items: center;
    display: flex;
    font-size: 12px;
    padding: 0 4px;
    color: #262626;
`;

interface Props {
    field: SchemaField;
}

export default function MenuColumn({ field }: Props) {
    const routeToTab = useRouteToTab();
    const { urn } = useEntityData();
    const selectedColumnUrn = generateSchemaFieldUrn(field.fieldPath, urn);

    return (
        <Dropdown
            overlay={
                <Menu>
                    <Menu.Item key="0">
                        <MenuItem
                            onClick={() => routeToTab({ tabName: 'Lineage', tabParams: { column: field.fieldPath } })}
                        >
                            <ImpactAnalysisIcon /> &nbsp; See Column Lineage
                        </MenuItem>
                    </Menu.Item>
                    {navigator.clipboard && (
                        <Menu.Item key="1">
                            <MenuItem
                                onClick={() => {
                                    navigator.clipboard.writeText(field.fieldPath);
                                }}
                            >
                                <CopyOutlinedIcon /> &nbsp; Copy Column Field Path
                            </MenuItem>
                        </Menu.Item>
                    )}
                    {navigator.clipboard && (
                        <Menu.Item key="2">
                            <MenuItem
                                onClick={() => {
                                    navigator.clipboard.writeText(selectedColumnUrn || '');
                                }}
                            >
                                <CopyOutlinedIcon /> &nbsp; Copy Column Urn
                            </MenuItem>
                        </Menu.Item>
                    )}
                </Menu>
            }
            trigger={['click']}
        >
            <MenuIcon fontSize={16} />
        </Dropdown>
    );
}
