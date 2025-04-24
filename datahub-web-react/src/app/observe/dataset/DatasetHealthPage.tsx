import { PageTitle, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { HeaderContainer, PageContainer } from '@app/govern/structuredProperties/styledComponents';
import { AssertionsSummary } from '@app/observe/dataset/assertion/AssertionsSummary';
import { IncidentsSummary } from '@app/observe/dataset/incident/IncidentsSummary';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';

import { useGetTotalDatasetsQuery } from '@graphql/dataset_health.generated';
import { EntityType } from '@types';

const Content = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => (props.$isShowNavBarRedesign ? 'white' : colors.white)};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        overflow-y: auto;
        height: calc(100% - 115px);
    `}
`;

const Section = styled.div`
    background-color: white;
    border-radius: 8px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    margin-bottom: 40px;
    border: 1px solid ${colors.gray[100]};
`;

/**
 * The top-level Dataset Health Dashboard which lives under the Observe module.
 */
export const DatasetHealthPage = () => {
    // The total number of datasets in the instance (the denominator for metrics).
    const userContext = useUserContext();
    const viewUrn = userContext.localState?.selectedViewUrn;
    const isShowNavBarRedesign = useShowNavBarRedesign();

    const { data } = useGetTotalDatasetsQuery({
        variables: {
            input: {
                query: '*',
                types: [EntityType.Dataset],
                start: 0,
                count: 0,
                viewUrn,
            },
        },
    });

    const total = data?.searchAcrossEntities?.total || 0;

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <HeaderContainer>
                <PageTitle title="Dataset Health" subTitle="Monitor the health of your organization's datasets" />
            </HeaderContainer>
            <Content $isShowNavBarRedesign={isShowNavBarRedesign}>
                <Section>
                    <IncidentsSummary total={total} />
                </Section>

                <Section>
                    <AssertionsSummary total={total} />
                </Section>
            </Content>
        </PageContainer>
    );
};
