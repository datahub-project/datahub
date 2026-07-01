import { Share } from '@phosphor-icons/react/dist/csr/Share';
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
                <Share size={16} weight="regular" />
            </Dropdown>
        </ActionMenuItem>
    );
}
