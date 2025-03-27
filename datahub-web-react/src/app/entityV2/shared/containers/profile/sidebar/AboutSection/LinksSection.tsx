import React, { useMemo } from 'react';
import styled from 'styled-components';
import LinkButton from '../LinkButton';
import { useEntityData, useRefetch } from '../../../../../../entity/shared/EntityContext';
import { AddLinkModal } from '../../../../components/styled/AddLinkModal';
import { shouldTryLinkPreview } from '../../../../../../integration/linkPreviews';
import LinkPreview from '../../../../../../integration/LinkPreview';

const AddLinksWrapper = styled.div`
    margin-left: -15px;
`;

interface Props {
    readOnly?: boolean;
}

export default function LinksSection({ readOnly }: Props) {
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
                    {shouldTryLinkPreview(link.url) && <LinkPreview link={link} />}
                </>
            ))}
            {!readOnly && (
                <AddLinksWrapper>
                    <AddLinkModal buttonProps={{ type: 'text' }} refetch={refetch} />
                </AddLinksWrapper>
            )}
        </>
    );
}
