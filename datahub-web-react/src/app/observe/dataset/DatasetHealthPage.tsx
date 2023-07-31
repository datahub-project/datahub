import React from 'react';
import styled from 'styled-components';
import { DatabaseOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import { useGetTotalDatasetsQuery } from '../../../graphql/dataset_health.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import { AssertionsSummary } from './assertion/AssertionsSummary';
import { IncidentsSummary } from './incident/IncidentsSummary';

const Container = styled.div`
    height: 100%;
`;

const Header = styled.div`
    && {
        padding-left: 40px;
        padding-right: 40px;
        padding-bottom: 12px;
        padding-top: 20px;
    }
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
`;

const Title = styled(Typography.Title)`
    && {
        margin-bottom: 12px;
    }
`;

const SubTitle = styled(Typography.Paragraph)`
    && {
        font-size: 16px;
    }
`;

const Content = styled.div`
    padding-left: 40px;
    padding-right: 40px;
    padding-top: 20px;
    background-color: ${ANTD_GRAY[2]};
`;

const ContentSectionTitle = styled(Typography.Title)`
    padding-left: 20px;
`;

const ContentSection = styled.div`
    height: 100%;
    display: flex;
`;

const Section = styled.div`
    background-color: white;
    border-radius: 8px;
    box-shadow: ${(props) => props.theme.styles['box-shadow']};
    margin-bottom: 40px;
    border: 1px solid ${ANTD_GRAY[4]};
`;

const LeftColumn = styled.div`
    width: 50%;
    padding: 20px;
`;

const RightColumn = styled.div`
    width: 50%;
    padding: 20px;
`;

const StyledDatabaseOutlined = styled(DatabaseOutlined)`
    && {
        font-size: 18px;
        padding-right: 12px;
    }
`;

const PAGE_TITLE = 'Dataset Health';
const PAGE_SUB_TITLE = "Monitor the health of your organization's datasets";

/**
 * The top-level Dataset Health Dashboard which lives under the Observe module.
 */
export const DatasetHealthPage = () => {
    // The total number of datasets in the instance (the denominator for metrics).
    const { data } = useGetTotalDatasetsQuery();
    const total = data?.searchAcrossEntities?.total || 0;

    return (
        <Container>
            <Header>
                <Title level={3}>
                    <StyledDatabaseOutlined />
                    {PAGE_TITLE}
                </Title>
                <SubTitle type="secondary">{PAGE_SUB_TITLE}</SubTitle>
            </Header>
            <Content>
                <ContentSectionTitle level={3}>Overview</ContentSectionTitle>
                <ContentSection>
                    <LeftColumn>
                        <Section>
                            <IncidentsSummary total={total} />
                        </Section>
                    </LeftColumn>
                    <RightColumn>
                        <Section>
                            <AssertionsSummary total={total} />
                        </Section>
                    </RightColumn>
                </ContentSection>
            </Content>
        </Container>
    );
};
