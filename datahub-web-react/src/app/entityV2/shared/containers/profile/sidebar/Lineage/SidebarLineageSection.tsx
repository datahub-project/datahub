import { Button, Icon, Tooltip } from '@components';
import React, { useContext } from 'react';
import { useHistory } from 'react-router-dom';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import SidebarLineageLoadingSection from '@app/entityV2/shared/containers/profile/sidebar/Lineage/SidebarLineageLoadingSection';
import {
    getDirectDownstreamSummary,
    getDirectUpstreamSummary,
    getRelatedEntitySummary,
} from '@app/entityV2/shared/containers/profile/sidebar/Lineage/utils';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { useGetDefaultLineageStartTimeMillis } from '@app/lineage/utils/useGetLineageTimeParams';
import { useEntityRegistry } from '@app/useEntityRegistry';
import UpstreamHealth from '@src/app/entityV2/shared/embed/UpstreamHealth/UpstreamHealth';
import CompactContext from '@src/app/shared/CompactContext';

import { useGetSearchAcrossLineageCountsQuery } from '@graphql/lineage.generated';

const Section = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    margin-bottom: 6px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const DirectionText = styled.div`
    font-size: 10px;
    font-weight: 700;
    line-height: 20px;
    letter-spacing: 0.48px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

const SummaryText = styled.div`
    text-wrap: wrap;
    font-size: 12px;
    font-weight: 600;
    line-height: 20px;
`;

const DirectionIcon = styled.span`
    margin-right: 4px;
    display: flex;
    align-items: center;
`;

const DirectionHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    font-weight: bold;
    font-size: 12px;
    letter-spacing: 1px;
    height: 20px;
    color: ${(props) => props.theme.colors.textTertiary};
    min-width: 100px;
    margin-right: 6px;
`;

const SidebarLineageSection = () => {
    const { urn, entityData, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const isCompact = useContext(CompactContext);
    const startTimeMillis = useGetDefaultLineageStartTimeMillis();

    const separateSiblings = useIsSeparateSiblingsMode();
    const onCombinedSiblingPage = !separateSiblings && (entityData?.siblingsSearch?.total || 0) > 0;
    const { data, loading } = useGetSearchAcrossLineageCountsQuery({
        variables: { urn, startTimeMillis },
        fetchPolicy: 'cache-first',
        skip: onCombinedSiblingPage,
    });

    const directUpstreamSummary = data?.upstreams && getDirectUpstreamSummary(data.upstreams as any);
    const directDownstreamSummary = data?.downstreams && getDirectDownstreamSummary(data.downstreams as any);

    const directUpstreamCount = directUpstreamSummary?.total || 0;
    const directDownstreamCount = directDownstreamSummary?.total || 0;

    const hasLineage = directUpstreamCount > 0 || directDownstreamCount > 0;

    if (!hasLineage) {
        return null;
    }

    return (
        <SidebarSection
            title="Lineage"
            key="Lineage"
            content={
                <>
                    {loading && <SidebarLineageLoadingSection />}
                    {!loading && <UpstreamHealth />}
                    {!loading && directUpstreamCount > 0 && (
                        <Section key="upstream">
                            <Tooltip
                                title="Data assets that this is directly derived from"
                                placement="left"
                                showArrow={false}
                            >
                                <DirectionHeader>
                                    <DirectionIcon>
                                        <Icon icon="ArrowUp" source="phosphor" size="md" />
                                    </DirectionIcon>
                                    <DirectionText>UPSTREAM</DirectionText>
                                </DirectionHeader>
                            </Tooltip>
                            <SummaryText>
                                Depends on {getRelatedEntitySummary(directUpstreamSummary as any, entityRegistry)}
                            </SummaryText>
                        </Section>
                    )}
                    {!loading && directDownstreamCount > 0 && (
                        <Section key="downstream">
                            <Tooltip
                                title="Data assets that directly depend on this"
                                placement="left"
                                showArrow={false}
                            >
                                <DirectionHeader>
                                    <DirectionIcon>
                                        <Icon icon="ArrowDown" source="phosphor" size="md" />
                                    </DirectionIcon>
                                    <DirectionText>DOWNSTREAM</DirectionText>
                                </DirectionHeader>
                            </Tooltip>
                            <SummaryText>
                                Used by {getRelatedEntitySummary(directDownstreamSummary as any, entityRegistry)}
                            </SummaryText>
                        </Section>
                    )}
                </>
            }
            extra={
                <Tooltip title="Explore related entities using the lineage graph" placement="left" showArrow={false}>
                    <Button
                        variant="text"
                        color="violet"
                        size="md"
                        icon={{ icon: 'TreeStructure', source: 'phosphor' }}
                        onClick={(e) => {
                            e.stopPropagation();
                            const lineagePath = `${entityRegistry.getEntityUrl(entityType, urn)}/Lineage`;
                            if (isCompact) {
                                window.open(lineagePath, '_blank');
                            } else {
                                history.push(lineagePath);
                            }
                        }}
                    />
                </Tooltip>
            }
        />
    );
};

export default SidebarLineageSection;
