import React from 'react';
import styled from 'styled-components';
import LinkButton from '../LinkButton';
import { useEntityData, useRefetch } from '../../../../EntityContext';
import { AddLinkModal } from '../../../../components/styled/AddLinkModal';

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

    const links = entityData?.institutionalMemory?.elements || [];

    return (
        <>
            {links.map((link) => (
                <LinkButton link={link} />
            ))}
            {!readOnly && !hideLinksButton && (
                <AddLinksWrapper>
                    <AddLinkModal buttonProps={{ type: 'text' }} refetch={refetch} />
                </AddLinksWrapper>
            )}
        </>
    );
}
