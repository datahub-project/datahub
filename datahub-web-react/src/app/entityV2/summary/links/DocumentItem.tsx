import { Icon, Text, Tooltip, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import AvatarPillWithLinkAndHover from '@components/components/Avatar/AvatarPillWithLinkAndHover';

import { formatDateString } from '@app/entityV2/shared/containers/profile/utils';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Document } from '@types';

const DocumentContainer = styled.div`
    display: flex;
    width: 100%;
    border-radius: 8px;
    background-color: ${colors.gray[1500]};
    justify-content: space-between;
    padding: 8px 4px;
    cursor: pointer;
`;

const LeftSection = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
    flex: 1;
    min-width: 0; /* Allows flex item to shrink below its content size, enabling truncation */
`;

const RightSection = styled.div`
    display: flex;
    gap: 8px;
    align-items: center;
    justify-content: flex-end;
    flex-shrink: 0; /* Prevents right section from shrinking */
    margin-left: 8px; /* Adds spacing between title and right section */
`;

type Props = {
    document: Document;
    onClick: (documentUrn: string) => void;
};

export default function DocumentItem({ document, onClick }: Props) {
    const entityRegistry = useEntityRegistryV2();
    const title = document.info?.title || 'Untitled Document';
    const lastModified = document.info?.lastModified;
    const actor = lastModified?.actor;

    const handleClick = (e: React.MouseEvent) => {
        e.preventDefault();
        onClick(document.urn);
    };

    return (
        <DocumentContainer onClick={handleClick} data-testid={`${document.urn}-${title}`}>
            <LeftSection>
                <Icon icon="FileText" source="phosphor" color="primary" size="lg" />
                <Text
                    style={{
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                        flex: 1,
                        minWidth: 0 /* Critical for truncation in flex containers */,
                    }}
                    color="primary"
                    lineHeight="normal"
                    data-testid="document-label"
                >
                    {title}
                </Text>
            </LeftSection>
            <RightSection>
                {lastModified?.time && (
                    <>
                        <Text color="gray" size="sm">
                            Edited{' '}
                            <Tooltip title={formatDateString(lastModified.time)}>
                                <span>{toRelativeTimeString(lastModified.time) || 'recently'}</span>
                            </Tooltip>
                            {actor && ' by '}
                        </Text>
                        {actor && <AvatarPillWithLinkAndHover user={actor} size="sm" entityRegistry={entityRegistry} />}
                    </>
                )}
            </RightSection>
        </DocumentContainer>
    );
}
