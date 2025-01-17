import { useIsSeparateSiblingsMode } from '@app/entityV2/shared/useIsSeparateSiblingsMode';
import { useGetDefaultLineageStartTimeMillis } from '@app/lineage/utils/useGetLineageTimeParams';
import React from 'react';
import { Link } from 'react-router-dom';
import styled from 'styled-components/macro';
import { Tooltip } from '@components';
import { ArrowDownOutlined, ArrowUpOutlined, PartitionOutlined } from '@ant-design/icons';
import UpstreamHealth from '@src/app/entityV2/shared/embed/UpstreamHealth/UpstreamHealth';
import { useGetSearchAcrossLineageCountsQuery } from '../../../../../../../graphql/lineage.generated';
import { useEntityData } from '../../../../../../entity/shared/EntityContext';
import { SidebarSection } from '../SidebarSection';
import { getDirectDownstreamSummary, getDirectUpstreamSummary, getRelatedEntitySummary } from './utils';
import SidebarLineageLoadingSection from './SidebarLineageLoadingSection';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { ANTD_GRAY, REDESIGN_COLORS } from '../../../../constants';
import SectionActionButton from '../SectionActionButton';
import { useEmbeddedProfileLinkProps } from '../../../../../../shared/useEmbeddedProfileLinkProps';

const Section = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    margin-bottom: 6px;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

const DirectionText = styled.div`
    font-size: 10px;
    font-weight: 700;
    line-height: 20px;
    letter-spacing: 0.48px;
    color: ${REDESIGN_COLORS.DARK_GREY};
`;

const SummaryText = styled.div`
    text-wrap: wrap;
    font-size: 12px;
    font-weight: 600;
    line-height: 20px;
`;

const StyledUpOutlined = styled(ArrowUpOutlined)`
    margin-right: 4px;
`;

const StyledDownOutlined = styled(ArrowDownOutlined)`
    margin-right: 4px;
`;

const DirectionHeader = styled.div`
    display: flex;
    align-items: center;
    justify-content: start;
    font-weight: bold;
    font-size: 12px;
    letter-spacing: 1px;
    height: 20px;
    color: ${ANTD_GRAY[6]};
    min-width: 100px;
    margin-right: 6px;
`;

const StyledPartitionOutlined = styled(PartitionOutlined)`
    svg {
        padding: 4px 5px 4px 4px;
    }
`;

const SidebarLineageSection = () => {
    const { urn, entityData, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const linkProps = useEmbeddedProfileLinkProps();
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
                                    <StyledUpOutlined />
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
                                    <StyledDownOutlined />
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
                <SectionActionButton
                    button={
                        <Tooltip
                            title="Explore related entities using the lineage graph"
                            placement="left"
                            showArrow={false}
                        >
                            <Link to={`${entityRegistry.getEntityUrl(entityType, urn)}/Lineage`} {...linkProps}>
                                <StyledPartitionOutlined />
                            </Link>
                        </Tooltip>
                    }
                    onClick={(e) => e.stopPropagation()}
                />
            }
        />
    );
};

export default SidebarLineageSection;
