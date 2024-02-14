import React from 'react';
import styled from 'styled-components/macro';
import { Typography } from 'antd';
import { useHistory } from 'react-router';
import { useEntityData } from '../../../../EntityContext';
import { SidebarSection } from '../SidebarSection';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { getContentsSummary, getContentsSummaryText, navigateToDomainEntities } from './utils';
import { useGetDomainEntitySummaryQuery } from '../../../../../../../graphql/domain.generated';
import SidebarEntitiesLoadingSection from './SidebarEntitiesLoadingSection';

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

const SidebarEntitiesSection = () => {
    const { urn, entityType } = useEntityData();
    const entityRegistry = useEntityRegistry();
    const history = useHistory();
    const { data, loading } = useGetDomainEntitySummaryQuery({
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
                    {loading && <SidebarEntitiesLoadingSection />}
                    {!loading &&
                        (hasContents ? (
                            <>
                                <Section>
                                    <SummaryText>
                                        {getContentsSummaryText(contentsSummary as any, entityRegistry)}
                                    </SummaryText>
                                    <ViewAllButton
                                        onClick={() =>
                                            navigateToDomainEntities(urn, entityType, history, entityRegistry)
                                        }
                                    >
                                        View all
                                    </ViewAllButton>
                                </Section>
                            </>
                        ) : (
                            <Typography.Text type="secondary">No contents yet</Typography.Text>
                        ))}
                </>
            }
        />
    );
};

export default SidebarEntitiesSection;
