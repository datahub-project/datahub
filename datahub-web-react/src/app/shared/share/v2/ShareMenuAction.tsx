import React from 'react';
import { Dropdown } from 'antd';
import { ShareAltOutlined } from '@ant-design/icons';
import ShareButtonMenu from '../ShareButtonMenu';
import { ActionMenuItem } from '../../../entityV2/shared/EntityDropdown/styledComponents';
import { useEntityData } from '../../../entityV2/shared/EntityContext';

export default function ShareMenuAction() {
    const { urn, entityType, entityData } = useEntityData();
    const subType = (entityData?.subTypes?.typeNames?.length && entityData?.subTypes?.typeNames?.[0]) || undefined;
    const name = entityData?.name;

    return (
        <ActionMenuItem key="share">
            <Dropdown
                trigger={['hover']}
                overlay={<ShareButtonMenu urn={urn} entityType={entityType} subType={subType} name={name} />}
            >
                <ShareAltOutlined style={{ display: 'flex' }} />
            </Dropdown>
        </ActionMenuItem>
    );
}
