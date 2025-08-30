import { Icon, Text, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import AvatarPillWithLinkAndHover from '@components/components/Avatar/AvatarPillWithLinkAndHover';

import { formatDateString } from '@app/entityV2/shared/containers/profile/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { InstitutionalMemoryMetadata } from '@types';

const LinkContainer = styled.div`
    display: flex;
    width: 100%;
    border-radius: 8px;
    background-color: ${colors.gray[1500]};
    justify-content: space-between;
    padding: 8px 4px;
`;

const LeftSection = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
`;

const RightSection = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
`;

const StyledIcon = styled(Icon)`
    :hover {
        cursor: pointer;
    }
`;

type Props = {
    link: InstitutionalMemoryMetadata;
    setSelectedLink: (link: InstitutionalMemoryMetadata | null) => void;
    setShowConfirmDelete: (show: boolean) => void;
};

export default function LinkItem({ link, setSelectedLink, setShowConfirmDelete }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const createdBy = link.actor;

    return (
        <a href={link.url} target="_blank" rel="noreferrer">
            <LinkContainer>
                <LeftSection>
                    <Icon icon="LinkSimple" source="phosphor" color="primary" size="lg" />
                    <Text color="primary" lineHeight="normal">
                        {link.description || link.label}
                    </Text>
                </LeftSection>
                <RightSection>
                    <Text color="gray" size="sm">
                        Added {formatDateString(link.created.time)} by{' '}
                    </Text>
                    <AvatarPillWithLinkAndHover user={createdBy} size="sm" entityRegistry={entityRegistry} />
                    <StyledIcon icon="PencilSimpleLine" source="phosphor" color="gray" size="md" />
                    <StyledIcon
                        icon="Trash"
                        source="phosphor"
                        color="red"
                        size="md"
                        onClick={(e) => {
                            e.preventDefault();
                            setSelectedLink(link);
                            setShowConfirmDelete(true);
                        }}
                    />
                </RightSection>
            </LinkContainer>
        </a>
    );
}
