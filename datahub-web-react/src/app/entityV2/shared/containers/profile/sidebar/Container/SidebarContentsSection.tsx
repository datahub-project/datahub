import React from 'react';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import { useHistory } from 'react-router';
import { useEntityData } from '../../../../EntityContext';
import { SidebarSection } from '../SidebarSection';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { getContentsSummary, getContentsSummaryText, navigateToContainerContents } from './utils';
import { useGetContainerEntitySummaryQuery } from '../../../../../../../graphql/container.generated';
import SidebarContentsLoadingSection from './SidebarContentsLoadingSection';

const Section = styled.div`
    display: flex;
    align-items: start;
    justify-content: start;
    flex-wrap: wrap;
`;

const SummaryText = styled.div`
    margin-right: 8px;
    text-wrap: wrap;
`;

const ViewAllButton = styled.div`
    display: flex;
    align-items: center;
    font-weight: bold;
    padding: 0px 2px;
    :hover {
        cursor: pointer;
    }
`;

const SidebarContentsSection = () => {
    const { urn, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const { data, loading } = useGetContainerEntitySummaryQuery({
        variables: {
            urn,
        },
        fetchPolicy: 'cache-first',
    });

    const contentsSummary = data?.aggregateAcrossEntities && getContentsSummary(data.aggregateAcrossEntities as any);
    const contentsCount = contentsSummary?.total || 0;
    const hasContents = contentsCount > 0;

    return (
        <SidebarSection
            title="Contents"
            key="Contents"
            content={
                <>
                    {loading && <SidebarContentsLoadingSection />}
                    {!loading &&
                        (hasContents ? (
                            <Section>
                                <SummaryText>
                                    {getContentsSummaryText(contentsSummary as any, entityRegistry)}
                                </SummaryText>
                                <ViewAllButton
                                    onClick={() =>
                                        navigateToContainerContents(urn, entityType, history, entityRegistry)
                                    }
                                >
                                    View all
                                </ViewAllButton>
                            </Section>
                        ) : (
                            <Typography.Text type="secondary">No contents yet</Typography.Text>
                        ))}
                </>
            }
        />
    );
};

export default SidebarContentsSection;
