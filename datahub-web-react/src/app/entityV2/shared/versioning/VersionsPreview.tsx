import { Pill, Text } from '@components';
import React from 'react';
import { Link } from 'react-router-dom';
import styled, { useTheme } from 'styled-components';

import analytics, { EventType } from '@app/analytics';
import { useEntityContext, useEntityData } from '@app/entity/shared/EntityContext';
import { DrawerType } from '@app/entity/shared/types';
import { VersionPill } from '@app/entityV2/shared/versioning/common';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, VersionSet } from '@types';

const Wrapper = styled.div`
    display: flex;
    flex-direction: column;
    gap: 4px;

    min-width: 200px;
`;

const Header = styled(Text)`
    display: flex;
    align-items: center;
    justify-content: start;
    gap: 6px;
    margin-bottom: 8px;

    color: ${(props) => props.theme.colors.text};
    font-size: 14px;
    line-height: 1.2;
`;

const VersionsCount = styled(Pill)`
    line-height: 1.2;
`;

const VersionsWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

const Footer = styled.div`
    display: flex;
    justify-content: end;
    padding: 4px 0;
`;

const ShowAllButton = styled(Text)`
    cursor: pointer;
`;

interface Props {
    versionSet?: VersionSet;
}

export default function VersionsPreview({ versionSet }: Props) {
    const theme = useTheme();
    const { urn, entityType, setDrawer } = useEntityContext();

    const count = versionSet?.versionsSearch?.count;
    const total = versionSet?.versionsSearch?.total;
    return (
        <Wrapper>
            <Header size="xl" type="div">
                <Text weight="semiBold">Versions</Text>
                {!!total && <VersionsCount label={total.toString()} size="sm" clickable={false} />}
            </Header>
            <VersionsWrapper>
                {versionSet?.versionsSearch?.searchResults?.map((result) => (
                    <VersionPreviewRow entity={result.entity} />
                ))}
            </VersionsWrapper>
            {!!count && !!total && total > count && setDrawer && (
                <Footer>
                    <ShowAllButton
                        size="md"
                        weight="bold"
                        style={{ color: theme.colors.textSecondary }}
                        onClick={() => {
                            analytics.event({
                                type: EventType.ShowAllVersionsEvent,
                                assetUrn: urn,
                                versionSetUrn: versionSet?.urn,
                                entityType,
                                numVersions: total,
                                uiLocation: 'preview',
                            });
                            setDrawer(DrawerType.VERSIONS);
                        }}
                    >
                        View all
                    </ShowAllButton>
                </Footer>
            )}
        </Wrapper>
    );
}

const VersionPreviewEntry = styled.div<{ isViewing: boolean }>`
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    padding: 8px;

    ${(props) => props.isViewing && `background: ${props.theme.colors.bgSelectedSubtle}`};
`;

const VersionPreviewHeader = styled.span`
    display: flex;
    align-items: center;
    gap: 8px;
`;

interface VersionPreviewRowProps {
    entity: Entity;
}

function VersionPreviewRow({ entity }: VersionPreviewRowProps) {
    const theme = useTheme();
    const entityRegistry = useEntityRegistry();
    const { urn: entityProfileUrn } = useEntityData();

    const versionProperties = entityRegistry.getGenericEntityProperties(entity.type, entity)?.versionProperties;

    const isViewing = entity.urn === entityProfileUrn;
    return (
        <VersionPreviewEntry isViewing={isViewing}>
            <VersionPreviewHeader>
                <VersionPill
                    label={versionProperties?.version?.versionTag ?? '<unlabeled>'}
                    isLatest={versionProperties?.isLatest}
                />
                {!!versionProperties?.isLatest && (
                    <Text size="md" style={{ color: theme.colors.textSecondary }}>
                        Latest
                    </Text>
                )}
            </VersionPreviewHeader>
            {isViewing && (
                <Text size="md" weight="semiBold" style={{ color: theme.colors.textTertiary }}>
                    Viewing
                </Text>
            )}
            {!isViewing && (
                <Link to={entityRegistry.getEntityUrl(entity.type, entity.urn)}>
                    <Text size="md" color="textBrand" weight="semiBold">
                        View
                    </Text>
                </Link>
            )}
        </VersionPreviewEntry>
    );
}
