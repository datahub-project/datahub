import { Icon, typography } from '@components';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { Dropdown } from 'antd';
import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';

import { CardIcons } from '@app/govern/structuredProperties/styledComponents';
import { OwnerAvatarGroup } from '@app/sharedV2/owners/OwnerAvatarGroup';
import { useEntityRegistry, useEntityRegistryV2 } from '@app/useEntityRegistry';
import { EntityType, Ownership } from '@src/types.generated';

const ApplicationName = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: ${(props) => props.theme.colors.text};
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    cursor: pointer;
    &:hover {
        text-decoration: underline;
    }
`;

const ApplicationDescription = styled.div`
    font-size: 14px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.textSecondary};
    white-space: normal;
    line-height: 1.4;
`;

const ColumnContainer = styled.div`
    display: flex;
    flex-direction: column;
    max-width: 300px;
    width: 100%;
`;

const MenuItem = styled.div`
    display: flex;
    padding: 5px 70px 5px 5px;
    font-size: 14px;
    font-weight: 400;
    color: ${(props) => props.theme.colors.text};
    font-family: ${typography.fonts.body};
`;

const DeleteMenuItem = styled(MenuItem)`
    color: ${(props) => props.theme.colors.textError};
`;

export const ApplicationNameColumn = React.memo(
    ({
        applicationUrn,
        displayName,
        searchQuery,
    }: {
        applicationUrn: string;
        displayName: string;
        searchQuery?: string;
    }) => {
        const entityRegistry = useEntityRegistry();
        const url = entityRegistry.getEntityUrl(EntityType.Application, applicationUrn);

        return (
            <ColumnContainer>
                <ApplicationName onClick={() => window.open(url, '_blank')} data-testid={`${applicationUrn}-name`}>
                    <Highlight search={searchQuery}>{displayName}</Highlight>
                </ApplicationName>
            </ColumnContainer>
        );
    },
);

export const ApplicationDescriptionColumn = React.memo(
    ({ applicationUrn, description }: { applicationUrn: string; description: string }) => {
        return (
            <ColumnContainer>
                <ApplicationDescription data-testid={`${applicationUrn}-description`}>
                    {description}
                </ApplicationDescription>
            </ColumnContainer>
        );
    },
);

export const ApplicationOwnersColumn = React.memo(({ owners }: { owners: Ownership }) => {
    const entityRegistry = useEntityRegistryV2();
    const ownerList = owners?.owners || [];

    if (ownerList.length === 0) return <>-</>;

    return (
        <ColumnContainer>
            <OwnerAvatarGroup owners={ownerList} entityRegistry={entityRegistry} />
        </ColumnContainer>
    );
});

export const ApplicationActionsColumn = React.memo(
    ({ applicationUrn, onDelete }: { applicationUrn: string; onDelete: () => void }) => {
        const entityRegistry = useEntityRegistry();
        const url = entityRegistry.getEntityUrl(EntityType.Application, applicationUrn);

        const items = [
            {
                key: '0',
                label: (
                    <MenuItem onClick={() => window.open(url, '_blank')} data-testid="action-edit">
                        View
                    </MenuItem>
                ),
            },
            {
                key: '1',
                label: (
                    <MenuItem
                        onClick={() => {
                            navigator.clipboard.writeText(applicationUrn);
                        }}
                    >
                        Copy Urn
                    </MenuItem>
                ),
            },
            {
                key: '2',
                label: (
                    <DeleteMenuItem onClick={onDelete} data-testid="action-delete">
                        Delete
                    </DeleteMenuItem>
                ),
            },
        ];

        return (
            <CardIcons>
                <Dropdown menu={{ items }} trigger={['click']} data-testid={`${applicationUrn}-actions-dropdown`}>
                    <Icon icon={DotsThreeVertical} size="md" data-testid="MoreVertOutlinedIcon" />
                </Dropdown>
            </CardIcons>
        );
    },
);
