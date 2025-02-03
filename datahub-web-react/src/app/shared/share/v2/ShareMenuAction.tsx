import React from 'react';
import { Dropdown } from 'antd';
import { ShareAltOutlined } from '@ant-design/icons';
import { ActionMenuItem } from '../../../entityV2/shared/EntityDropdown/styledComponents';
import { useEntityData } from '../../../entity/shared/EntityContext';
import ShareButtonMenu from './ShareButtonMenu';
import { StyledMenu } from './styledComponents';

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
                        <ShareButtonMenu urn={urn} entityType={entityType} subType={subType} name={name} />
                    </StyledMenu>
                }
            >
                <ShareAltOutlined style={{ display: 'flex' }} />
            </Dropdown>
        </ActionMenuItem>
    );
}
