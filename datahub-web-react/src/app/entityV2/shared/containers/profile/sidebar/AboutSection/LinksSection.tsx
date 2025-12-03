import React, { useMemo } from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { AddLinkModal } from '@app/entityV2/shared/components/styled/AddLinkModal';
import LinkButton from '@app/entityV2/shared/containers/profile/sidebar/LinkButton';
import LinkPreview from '@app/integration/LinkPreview';
import { shouldTryLinkPreview } from '@app/integration/linkPreviews';

const AddLinksWrapper = styled.div`
    margin-left: -15px;
`;

interface Props {
    readOnly?: boolean;
}

export default function LinksSection({ readOnly }: Props) {
    const { entityData } = useEntityData();

    const links = useMemo(
        // Do not show links that shown in entity profile's header
        () => entityData?.institutionalMemory?.elements?.filter((link) => !link.settings?.showInAssetPreview) || [],
        [entityData],
    );

    return (
        <>
            {links.map((link) => (
                <>
                    <LinkButton link={link} />
                    {shouldTryLinkPreview(link.url) && <LinkPreview link={link} />}
                </>
            ))}
            {!readOnly && (
                <AddLinksWrapper>
                    <AddLinkModal buttonProps={{ type: 'text' }} />
                </AddLinksWrapper>
            )}
        </>
    );
}
