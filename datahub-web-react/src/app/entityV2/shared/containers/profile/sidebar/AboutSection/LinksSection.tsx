/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React, { useMemo } from 'react';
import styled from 'styled-components';

import { useEntityData, useRefetch } from '@app/entity/shared/EntityContext';
import { AddLinkModal } from '@app/entityV2/shared/components/styled/AddLinkModal';
import LinkButton from '@app/entityV2/shared/containers/profile/sidebar/LinkButton';

const AddLinksWrapper = styled.div`
    margin-left: -15px;
`;

interface Props {
    hideLinksButton?: boolean;
    readOnly?: boolean;
}

export default function LinksSection({ hideLinksButton, readOnly }: Props) {
    const { entityData } = useEntityData();
    const refetch = useRefetch();

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
                </>
            ))}
            {!readOnly && !hideLinksButton && (
                <AddLinksWrapper>
                    <AddLinkModal buttonProps={{ type: 'text' }} refetch={refetch} />
                </AddLinksWrapper>
            )}
        </>
    );
}
