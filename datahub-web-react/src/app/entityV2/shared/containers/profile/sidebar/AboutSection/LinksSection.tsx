import React, { useMemo } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { AddLinkModal } from '@app/entityV2/shared/components/styled/AddLinkModal';
import { ResourceLinkPill } from '@app/entityV2/shared/tabs/Documentation/components/ResourceLinkPill';

interface Props {
    hideLinksButton?: boolean;
    readOnly?: boolean;
}

const LinkPillsContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
`;

export default function LinksSection({ hideLinksButton, readOnly }: Props) {
    const { entityData } = useEntityData();

    const links = useMemo(
        // Do not show links that shown in entity profile's header
        () => entityData?.institutionalMemory?.elements?.filter((link) => !link.settings?.showInAssetPreview) || [],
        [entityData],
    );

    return (
        <>
            {links.length > 0 && (
                <LinkPillsContainer>
                    {links.map((link) => (
                        <ResourceLinkPill key={`link-${link.url}`} link={link} />
                    ))}
                </LinkPillsContainer>
            )}
            {!readOnly && !hideLinksButton && <AddLinkModal buttonProps={{ type: 'text' }} />}
        </>
    );
}
