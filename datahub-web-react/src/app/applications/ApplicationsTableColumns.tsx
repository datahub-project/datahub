import { Icon, colors, typography } from '@components';
import { Dropdown } from 'antd';
import React from 'react';
import Highlight from 'react-highlighter';
import styled from 'styled-components';

import { CardIcons } from '@app/govern/structuredProperties/styledComponents';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { ExpandedOwner } from '@src/app/entity/shared/components/styled/ExpandedOwner/ExpandedOwner';
import { EntityType, Ownership } from '@src/types.generated';

const ApplicationName = styled.div`
    font-size: 14px;
    font-weight: 600;
    color: ${colors.gray[600]};
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
    color: ${colors.gray[1700]};
    white-space: normal;
    line-height: 1.4;
`;

const OwnersContainer = styled.div`
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
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
    color: ${colors.gray[600]};
    font-family: ${typography.fonts.body};
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

export const ApplicationOwnersColumn = React.memo(
    ({ applicationUrn, owners }: { applicationUrn: string; owners: Ownership }) => {
        return (
            <ColumnContainer>
                <OwnersContainer>
                    {owners?.owners?.map((ownerItem) => (
                        <ExpandedOwner
                            key={ownerItem.owner?.urn}
                            entityUrn={applicationUrn}
                            owner={ownerItem}
                            hidePopOver
                        />
                    ))}
                </OwnersContainer>
            </ColumnContainer>
        );
    },
);

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
                    <MenuItem onClick={onDelete} data-testid="action-delete" style={{ color: colors.red[500] }}>
                        Delete
                    </MenuItem>
                ),
            },
        ];

        return (
            <CardIcons>
                <Dropdown menu={{ items }} trigger={['click']} data-testid={`${applicationUrn}-actions-dropdown`}>
                    <Icon icon="MoreVert" size="md" />
                </Dropdown>
            </CardIcons>
        );
    },
);
