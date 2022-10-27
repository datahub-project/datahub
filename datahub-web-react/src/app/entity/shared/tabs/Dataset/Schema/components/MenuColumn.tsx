import React from 'react';
import { VscGraphLeft } from 'react-icons/vsc';
import styled from 'styled-components/macro';
import { Dropdown, Menu } from 'antd';
import { MenuIcon } from '../../../../EntityDropdown/EntityDropdown';
import { useRouteToTab } from '../../../../EntityContext';
import { SchemaField } from '../../../../../../../types.generated';

export const ImpactAnalysisIcon = styled(VscGraphLeft)`
    transform: scaleX(-1);
    font-size: 18px;
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
                </Menu>
            }
            trigger={['click']}
        >
            <MenuIcon fontSize={16} />
        </Dropdown>
    );
}
