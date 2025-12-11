/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
