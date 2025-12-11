/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { ShareAltOutlined } from '@ant-design/icons';
import { Dropdown } from 'antd';
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { ActionMenuItem } from '@app/entityV2/shared/EntityDropdown/styledComponents';
import ShareButtonMenu from '@app/shared/share/v2/ShareButtonMenu';
import { StyledMenu } from '@app/shared/share/v2/styledComponents';

export default function ShareMenuAction() {
    const { urn, entityType, entityData } = useEntityData();
    const subType = (entityData?.subTypes?.typeNames?.length && entityData?.subTypes?.typeNames?.[0]) || undefined;
    const name = entityData?.name;

    return (
        <ActionMenuItem key="share">
            <Dropdown
                trigger={['hover']}
                overlay={
                    <StyledMenu selectable={false}>
                        <ShareButtonMenu
                            urn={urn}
                            entityType={entityType}
                            subType={subType}
                            name={name}
                            qualifiedName={entityData?.properties?.qualifiedName}
                        />
                    </StyledMenu>
                }
            >
                <ShareAltOutlined style={{ display: 'flex' }} />
            </Dropdown>
        </ActionMenuItem>
    );
}
