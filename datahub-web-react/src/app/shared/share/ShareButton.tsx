import React from 'react';
import styled from 'styled-components';
import { Button, Dropdown } from 'antd';
import { ShareAltOutlined } from '@ant-design/icons';
import { EntityType } from '../../../types.generated';
import shareButtonMenu from './ShareButtonMenu';

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
    return (
        <Dropdown trigger={['click']} menu={shareButtonMenu({ urn, entityType, subType, name })}>
            <StyledButton type="primary">
                <StyledShareIcon />
                Share
            </StyledButton>
        </Dropdown>
    );
}
