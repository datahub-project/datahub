import React from 'react';
import styled from 'styled-components';
import { DatabaseOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import { useShowNavBarRedesign } from '@src/app/useShowNavBarRedesign';
import { useGetTotalDatasetsQuery } from '../../../graphql/dataset_health.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { AssertionsSummary } from './assertion/AssertionsSummary';
import { IncidentsSummary } from './incident/IncidentsSummary';
import { EntityType } from '../../../types.generated';
import { useUserContext } from '../../context/useUserContext';
import { PageTitle } from '@components';
import { PageContainer, HeaderContainer } from '../../govern/structuredProperties/styledComponents';

const Container = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    height: 100%;
    background-color: white;
    border-radius: ${(props) =>
        props.$isShowNavBarRedesign ? props.theme.styles['border-radius-navbar-redesign'] : '8px'};
    ${(props) =>
        props.$isShowNavBarRedesign &&
        `
        overflow:hidden;
        margin: 5px;
        box-shadow: ${props.theme.styles['box-shadow-navbar-redesign']};
    `}
`;

const Content = styled.div<{ $isShowNavBarRedesign?: boolean }>`
    background-color: ${(props) => (props.$isShowNavBarRedesign ? 'white' : ANTD_GRAY[2])};
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
    border: 1px solid ${ANTD_GRAY[4]};
`;

const StyledDatabaseOutlined = styled(DatabaseOutlined)`
    && {
        font-size: 18px;
        padding-right: 12px;
    }
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
