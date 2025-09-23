import { ShareAltOutlined } from '@ant-design/icons';
import { Button, Dropdown } from 'antd';
import React from 'react';
import styled from 'styled-components';

import useShareButtonMenu from '@app/shared/share/useShareButtonMenu';

import { EntityType } from '@types';

interface ShareButtonProps {
    urn: string;
    entityType: EntityType;
    subType?: string | null;
    name?: string | null;
}

const StyledButton = styled(Button)`
    padding-left: 10px;
    padding-right: 10px;
`;

const StyledShareIcon = styled(ShareAltOutlined)`
    font-size: 14px;
`;

export default function ShareButton({ urn, entityType, subType, name }: ShareButtonProps) {
    const items = useShareButtonMenu({ urn, entityType, subType, name });

    return (
        <Dropdown trigger={['click']} menu={items}>
            <StyledButton type="primary">
                <StyledShareIcon />
                Share
            </StyledButton>
        </Dropdown>
    );
}
