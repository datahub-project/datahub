import { CheckOutlined, LinkOutlined } from '@ant-design/icons';
import { Text, Tooltip } from '@components';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
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
    const { t } = useTranslation('shared.share');
    const { t: tc } = useTranslation('common.actions');
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
            <Tooltip title={t('copyLink.tooltip')}>
                {isClicked ? <CheckOutlined /> : <StyledLinkOutlined />}
                <TextSpan>
                    <b>{tc('copyLink')}</b>
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
