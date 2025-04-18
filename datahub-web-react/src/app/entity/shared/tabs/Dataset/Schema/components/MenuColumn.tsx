import React from 'react';
import { VscGraphLeft } from 'react-icons/vsc';
import { CopyOutlined } from '@ant-design/icons';
import styled from 'styled-components/macro';
import { Dropdown } from 'antd';
import { MenuIcon } from '../../../../EntityDropdown/EntityDropdown';
import { useEntityData, useRouteToTab } from '../../../../EntityContext';
import { SchemaField } from '../../../../../../../types.generated';
import { generateSchemaFieldUrn } from '../../../Lineage/utils';
import { MenuItemStyle } from '../../../../../view/menu/item/styledComponent';

export const ImpactAnalysisIcon = styled(VscGraphLeft)`
    transform: scaleX(-1);
    font-size: 18px;
`;

export const CopyOutlinedIcon = styled(CopyOutlined)`
    transform: scaleX(-1);
    font-size: 16px;
`;

interface Props {
    field: SchemaField;
}

export default function MenuColumn({ field }: Props) {
    const routeToTab = useRouteToTab();
    const { urn } = useEntityData();
    const selectedColumnUrn = generateSchemaFieldUrn(field.fieldPath, urn);

    const items = [
        {
            key: 0,
            label: (
                <MenuItemStyle
                    onClick={() => routeToTab({ tabName: 'Lineage', tabParams: { column: field.fieldPath } })}
                >
                    <ImpactAnalysisIcon /> &nbsp; See Column Lineage
                </MenuItemStyle>
            ),
        },
        navigator.clipboard
            ? {
                  key: 1,
                  label: (
                      <MenuItemStyle
                          onClick={() => {
                              navigator.clipboard.writeText(field.fieldPath);
                          }}
                      >
                          <CopyOutlinedIcon /> &nbsp; Copy Column Field Path
                      </MenuItemStyle>
                  ),
              }
            : null,
        navigator.clipboard
            ? {
                  key: 2,
                  label: (
                      <MenuItemStyle
                          onClick={() => {
                              navigator.clipboard.writeText(selectedColumnUrn || '');
                          }}
                      >
                          <CopyOutlinedIcon /> &nbsp; Copy Column Urn
                      </MenuItemStyle>
                  ),
              }
            : null,
    ];

    return (
        <Dropdown menu={{ items }} trigger={['click']}>
            <MenuIcon fontSize={16} />
        </Dropdown>
    );
}
