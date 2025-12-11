/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { CheckOutlined, LinkOutlined } from '@ant-design/icons';
import { Text, Tooltip } from '@components';
import React, { useState } from 'react';
import styled from 'styled-components/macro';

import { StyledMenuItem } from '@app/shared/share/v2/styledComponents';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import { EntityType } from '@types';

interface CopyLinkMenuItemProps {
    key: string;
    urn: string;
    entityType: EntityType;
}

const TextSpan = styled.span`
    padding-left: 12px;
    margin-left: 0px !important;
`;

const StyledLinkOutlined = styled(LinkOutlined)`
    font-size: 14px;
`;

export default function CopyLinkMenuItem({ key, urn, entityType }: CopyLinkMenuItemProps) {
    /**
     * Whether button has been clicked
     */
    const { origin } = window.location;
    const entityRegistry = useEntityRegistryV2();

    const [isClicked, setIsClicked] = useState(false);

    const copyUrl = `${origin}${resolveRuntimePath(`${entityRegistry.getEntityUrl(entityType, urn)}`)}/`;

    return (
        <StyledMenuItem
            key={key}
            onClick={() => {
                navigator.clipboard.writeText(copyUrl);
                setIsClicked(true);
            }}
        >
            <Tooltip title="Copy a shareable link to this entity.">
                {isClicked ? <CheckOutlined /> : <StyledLinkOutlined />}
                <TextSpan>
                    <b>Copy Link</b>
                </TextSpan>
            </Tooltip>
        </StyledMenuItem>
    );
}

const SimpleMenuItem = styled(Text)`
    display: flex;
    align-items: center;
    gap: 12px;
`;

export function SimpleCopyLinkMenuItem({
    urn,
    entityType,
    text,
}: Pick<CopyLinkMenuItemProps, 'urn' | 'entityType'> & { text: string }) {
    const { origin } = window.location;
    const entityRegistry = useEntityRegistryV2();

    const [isClicked, setIsClicked] = useState(false);

    const copyUrl = `${origin}${resolveRuntimePath(`${entityRegistry.getEntityUrl(entityType, urn)}`)}/`;

    return (
        <SimpleMenuItem
            onClick={() => {
                navigator.clipboard.writeText(copyUrl);
                setIsClicked(true);
            }}
        >
            {isClicked ? <CheckOutlined /> : <StyledLinkOutlined />}
            {text}
        </SimpleMenuItem>
    );
}
