import analytics, { EventType } from '@app/analytics';
import { useEntityContext, useEntityData } from '@app/entity/shared/EntityContext';
import { DrawerType } from '@app/entity/shared/types';
import { VersionPill } from '@app/entityV2/shared/versioning/common';
import { useEntityRegistry } from '@app/useEntityRegistry';
import { colors, Pill, Text } from '@components';
import { Entity, VersionSet } from '@types';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components';

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

    color: ${colors.gray[600]};
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
                        color="gray"
                        weight="bold"
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

    ${({ isViewing }) =>
        isViewing &&
        'background: linear-gradient(180deg, rgba(83, 63, 209, 0.04) -3.99%, rgba(112, 94, 228, 0.04) 53.04%, rgba(112, 94, 228, 0.04) 100%)'};
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
                    <Text size="md" color="gray">
                        Latest
                    </Text>
                )}
            </VersionPreviewHeader>
            {isViewing && (
                <Text size="md" color="gray" colorLevel={1800} weight="semiBold">
                    Viewing
                </Text>
            )}
            {!isViewing && (
                <Link to={entityRegistry.getEntityUrl(entity.type, entity.urn)}>
                    <Text size="md" color="violet" weight="semiBold">
                        View
                    </Text>
                </Link>
            )}
        </VersionPreviewEntry>
    );
}
