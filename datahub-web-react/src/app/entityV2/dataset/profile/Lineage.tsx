import { Button, EmptyState, Heading } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import { useHistory, useLocation } from 'react-router-dom';
import styled from 'styled-components';

import { PreviewType } from '@app/entityV2/Entity';
import { navigateToLineageUrl } from '@app/lineageV3/utils/navigateToLineageUrl';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { DownstreamEntityRelationships, EntityType, UpstreamEntityRelationships } from '@types';

type Props = {
    upstreamLineage?: UpstreamEntityRelationships | null;
    downstreamLineage?: DownstreamEntityRelationships | null;
};

const ViewRawButtonContainer = styled.div`
    display: flex;
    justify-content: flex-end;
`;

const ListsContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 24px;
    width: 100%;
`;

const BorderedList = styled.div`
    border: 1px solid ${(props) => props.theme.colors.border};
    border-radius: 2px;
    padding: 16px 32px;
`;

const ListHeader = styled.div`
    padding: 12px 24px;
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
`;

const ListItem = styled.div`
    padding: 20px 24px 12px 24px;

    &:not(:last-child) {
        border-bottom: 1px solid ${(props) => props.theme.colors.border};
    }
`;

export default function Lineage({ upstreamLineage, downstreamLineage }: Props) {
    const { t } = useTranslation('entity.types');
    const { t: tAlchemy } = useTranslation('alchemy');
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const location = useLocation();
    const upstreamEntities = upstreamLineage?.entities?.map((entityRelationship) => entityRelationship?.entity);
    const downstreamEntities = downstreamLineage?.entities?.map((entityRelationship) => entityRelationship?.entity);

    return (
        <>
            <div>
                <ViewRawButtonContainer>
                    <Button
                        variant="outline"
                        onClick={() => navigateToLineageUrl({ location, history, isLineageMode: true })}
                    >
                        {t('dataset.viewGraph')}
                    </Button>
                </ViewRawButtonContainer>
            </div>
            <ListsContainer>
                <BorderedList style={{ marginTop: '24px' }}>
                    <ListHeader>
                        <Heading type="h3" size="4xl" weight="semiBold">
                            {t('dataset.upstream')}
                        </Heading>
                    </ListHeader>
                    {upstreamEntities?.length ? (
                        upstreamEntities.map((item) => (
                            <ListItem key={item?.urn}>
                                {entityRegistry.renderPreview(
                                    item?.type || EntityType.Dataset,
                                    PreviewType.PREVIEW,
                                    item,
                                )}
                            </ListItem>
                        ))
                    ) : (
                        <EmptyState title={tAlchemy('noData')} size="sm" />
                    )}
                </BorderedList>
                <BorderedList style={{ marginTop: '12px' }}>
                    <ListHeader>
                        <Heading type="h3" size="4xl" weight="semiBold">
                            {t('dataset.downstream')}
                        </Heading>
                    </ListHeader>
                    {downstreamEntities?.length ? (
                        downstreamEntities.map((item) => (
                            <ListItem key={item?.urn}>
                                {entityRegistry.renderPreview(
                                    item?.type || EntityType.Dataset,
                                    PreviewType.PREVIEW,
                                    item,
                                )}
                            </ListItem>
                        ))
                    ) : (
                        <EmptyState title={tAlchemy('noData')} size="sm" />
                    )}
                </BorderedList>
            </ListsContainer>
        </>
    );
}
