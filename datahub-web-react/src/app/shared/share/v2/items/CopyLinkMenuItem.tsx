import React, { useState } from 'react';
import styled from 'styled-components/macro';
import { CheckOutlined, LinkOutlined } from '@ant-design/icons';
import { Text, Tooltip } from '@components';
import { StyledMenuItem } from '../styledComponents';
import { EntityType } from '../../../../../types.generated';
import { useEntityRegistryV2 } from '../../../../useEntityRegistry';

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

    const copyUrl = `${origin}${entityRegistry.getEntityUrl(entityType, urn)}/`;

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

    const copyUrl = `${origin}${entityRegistry.getEntityUrl(entityType, urn)}/`;

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
