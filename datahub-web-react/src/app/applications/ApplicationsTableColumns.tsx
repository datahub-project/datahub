import { Button, Menu } from '@components';
import { Copy } from '@phosphor-icons/react/dist/csr/Copy';
import { DotsThreeVertical } from '@phosphor-icons/react/dist/csr/DotsThreeVertical';
import { Eye } from '@phosphor-icons/react/dist/csr/Eye';
import { Trash } from '@phosphor-icons/react/dist/csr/Trash';
import React from 'react';
import Highlight from 'react-highlighter';
import { useTranslation } from 'react-i18next';
import { useHistory } from 'react-router';
import styled from 'styled-components';

import { ItemType } from '@components/components/Menu/types';

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
        const history = useHistory();
        const url = entityRegistry.getEntityUrl(EntityType.Application, applicationUrn);

        return (
            <ColumnContainer>
                <ApplicationName onClick={() => history.push(url)} data-testid={`${applicationUrn}-name`}>
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
        const { t } = useTranslation('misc');
        const { t: tc } = useTranslation('common.actions');
        const entityRegistry = useEntityRegistry();
        const history = useHistory();
        const url = entityRegistry.getEntityUrl(EntityType.Application, applicationUrn);

        const items: ItemType[] = [
            {
                type: 'item',
                key: 'view',
                title: tc('view'),
                icon: Eye,
                onClick: () => history.push(url),
                dataTestId: 'action-edit',
            },
            {
                type: 'item',
                key: 'copy-urn',
                title: t('applications.copyUrn'),
                icon: Copy,
                onClick: () => {
                    navigator.clipboard.writeText(applicationUrn);
                },
            },
            {
                type: 'item',
                key: 'delete',
                title: tc('delete'),
                icon: Trash,
                danger: true,
                onClick: onDelete,
                dataTestId: 'action-delete',
            },
        ];

        return (
            <CardIcons>
                <Menu items={items} trigger={['click']} data-testid={`${applicationUrn}-actions-dropdown`}>
                    <Button
                        variant="text"
                        icon={{ icon: DotsThreeVertical, weight: 'bold', size: 'xl', color: 'icon' }}
                        isCircle
                        data-testid="MoreVertOutlinedIcon"
                    />
                </Menu>
            </CardIcons>
        );
    },
);
