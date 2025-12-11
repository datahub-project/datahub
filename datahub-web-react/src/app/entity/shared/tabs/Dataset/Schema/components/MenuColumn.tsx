/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CopyOutlined } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React from 'react';
import { VscGraphLeft } from 'react-icons/vsc';
import styled from 'styled-components/macro';

import { useEntityData, useRouteToTab } from '@app/entity/shared/EntityContext';
import { MenuIcon } from '@app/entity/shared/EntityDropdown/EntityDropdown';
import { generateSchemaFieldUrn } from '@app/entity/shared/tabs/Lineage/utils';
import { MenuItemStyle } from '@app/entity/view/menu/item/styledComponent';

import { SchemaField } from '@types';

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
