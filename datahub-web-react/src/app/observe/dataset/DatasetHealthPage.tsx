import { PageTitle, Tabs, colors } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useUserContext } from '@app/context/useUserContext';
import { HeaderContainer, PageContainer } from '@app/govern/structuredProperties/styledComponents';
import { AssertionsSummary } from '@app/observe/dataset/assertion/AssertionsSummary';
import { IncidentsSummary } from '@app/observe/dataset/incident/IncidentsSummary';
import { useAppConfig } from '@app/useAppConfig';
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
    const appConfig = useAppConfig();
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

    const assertionsTab = (
        <Tabs
            tabs={[
                {
                    component: <div>This is the content for Assertions - By Assertions</div>,
                    key: 'by-assertions',
                    name: 'By Assertions',
                },
                {
                    component: <div>This is the content for Assertions - By Table</div>,
                    key: 'by-table',
                    name: 'By Table',
                },
            ]}
        />
    );

    const mainTabs = (
        <Tabs
            tabs={[
                {
                    component: assertionsTab,
                    key: 'assertions',
                    name: 'Assertions',
                },
                {
                    component: <div>This is the content for Incidents</div>,
                    key: 'incidents',
                    name: 'Incidents',
                },
            ]}
        />
    );

    return (
        <PageContainer $isShowNavBarRedesign={isShowNavBarRedesign}>
            <HeaderContainer>
                <PageTitle title="Dataset Health" subTitle="Monitor the health of your organization's datasets" />
            </HeaderContainer>
            <Content $isShowNavBarRedesign={isShowNavBarRedesign}>
                {appConfig.config.featureFlags.datasetHealthDashboardV2Enabled ? mainTabs : null}
                {appConfig.config.featureFlags.datasetHealthDashboardV2Enabled ? null : (
                    <>
                        <Section>
                            <IncidentsSummary total={total} />
                        </Section>

                        <Section>
                            <AssertionsSummary total={total} />
                        </Section>
                    </>
                )}
            </Content>
        </PageContainer>
    );
};
