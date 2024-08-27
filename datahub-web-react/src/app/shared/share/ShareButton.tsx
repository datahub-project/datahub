import React from 'react';
import styled from 'styled-components';
import { Button, Dropdown } from 'antd';
import { ShareAltOutlined } from '@ant-design/icons';
import { EntityType } from '../../../types.generated';
import ShareButtonMenu from './ShareButtonMenu';

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
        <Dropdown
            trigger={['click']}
            overlay={
                <>
                    <ShareButtonMenu urn={urn} entityType={entityType} subType={subType} name={name} />
                </>
            }
        >
            <StyledButton type="primary">
                <StyledShareIcon />
                Share
            </StyledButton>
        </Dropdown>
    );
}
