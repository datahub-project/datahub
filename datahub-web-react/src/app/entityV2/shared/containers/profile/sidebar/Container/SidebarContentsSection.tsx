import React from 'react';
import { useHistory } from 'react-router';
import styled from 'styled-components/macro';

import { useEntityData } from '@app/entity/shared/EntityContext';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import SidebarContentsLoadingSection from '@app/entityV2/shared/containers/profile/sidebar/Container/SidebarContentsLoadingSection';
import {
    getContentsSummary,
    getContentsSummaryText,
    navigateToContainerContents,
} from '@app/entityV2/shared/containers/profile/sidebar/Container/utils';
import EmptySectionText from '@app/entityV2/shared/containers/profile/sidebar/EmptySectionText';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { useGetContainerEntitySummaryQuery } from '@graphql/container.generated';

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
    color: ${REDESIGN_COLORS.DARK_GREY};
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
                            <EmptySectionText message="No contents yet" />
                        ))}
                </>
            }
        />
    );
};

export default SidebarContentsSection;
