import React from 'react';
import styled from 'styled-components/macro';
import { useHistory } from 'react-router';
import { Tooltip } from 'antd';
import { ArrowDownOutlined, ArrowUpOutlined, PartitionOutlined } from '@ant-design/icons';
import { useGetSearchAcrossLineageCountsQuery } from '../../../../../../../graphql/lineage.generated';
import { useEntityData } from '../../../../EntityContext';
import { SidebarSection } from '../SidebarSection';
import {
    getDirectDownstreamSummary,
    getDirectUpstreamSummary,
    getRelatedEntitySummary,
    navigateToLineageGraph,
} from './utils';
import SidebarLineageLoadingSection from './SidebarLineageLoadingSection';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { ANTD_GRAY } from '../../../../constants';

const Section = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    margin-bottom: 6px;
`;

const DirectionText = styled.div``;

const SummaryText = styled.div`
    text-wrap: wrap;
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
    font-size: 10px;
    letter-spacing: 1px;
    height: 20px;
    && {
        color: ${ANTD_GRAY[7]};
    }
    width: 100px;
    margin-right: 6px;
`;

const ExploreButton = styled.div`
    display: flex;
    align-items: center;
    font-weight: bold;
    padding: 0px 2px;
    :hover {
        cursor: pointer;
    }
`;

const StyledPartitionOutlined = styled(PartitionOutlined)`
    && {
        font-size: 14px;
        margin-right: 8px;
    }
`;

const SidebarLineageSection = () => {
    const { urn, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const { data, loading } = useGetSearchAcrossLineageCountsQuery({
        variables: {
            urn,
        },
        fetchPolicy: 'cache-first',
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
                    <Tooltip
                        title="Explore related entities using the lineage graph"
                        placement="left"
                        showArrow={false}
                    >
                        <ExploreButton onClick={() => navigateToLineageGraph(urn, entityType, history, entityRegistry)}>
                            <StyledPartitionOutlined />
                            Explore
                        </ExploreButton>
                    </Tooltip>
                </>
            }
        />
    );
};

export default SidebarLineageSection;
