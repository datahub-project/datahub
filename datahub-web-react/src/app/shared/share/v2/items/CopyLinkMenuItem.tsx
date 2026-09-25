import { Text } from '@components';
import { Check } from '@phosphor-icons/react/dist/csr/Check';
import { LinkSimple } from '@phosphor-icons/react/dist/csr/LinkSimple';
import React, { useState } from 'react';
import styled from 'styled-components';

import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { resolveRuntimePath } from '@utils/runtimeBasePath';

import { EntityType } from '@types';

type Props = {
    urn: string;
    entityType: EntityType;
    text: string;
};

const SimpleMenuItem = styled(Text)`
    display: flex;
    align-items: center;
    gap: 12px;
    color: ${(props) => props.theme.colors.text};

    svg {
        color: ${(props) => props.theme.colors.icon};
    }
`;

export function SimpleCopyLinkMenuItem({ urn, entityType, text }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const [isClicked, setIsClicked] = useState(false);

    const copyUrl = `${window.location.origin}${resolveRuntimePath(entityRegistry.getEntityUrl(entityType, urn))}/`;

    return (
        <SimpleMenuItem
            onClick={() => {
                navigator.clipboard.writeText(copyUrl);
                setIsClicked(true);
            }}
        >
            {isClicked ? <Check size={14} /> : <LinkSimple size={14} />}
            {text}
        </SimpleMenuItem>
    );
}
