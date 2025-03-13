import React from 'react';
import styled from 'styled-components';
import { SafetyOutlined } from '@ant-design/icons';
import { ANTD_GRAY } from '../../../entity/shared/constants';
import { useGetOwnedAssetAssertionSummary } from './useGetOwnedAssetAssertionSummary';
import { useGetOwnedAssetIncidentSummary } from './useGetOwnedAssetIncidentSummary';
import { AssertionSummary } from './assertion/AssertionsSummary';
import { IncidentSummary } from './incident/IncidentSummary';

const Card = styled.div`
    border: 1px solid ${ANTD_GRAY[4]};
    border-radius: 11px;
    background-color: #ffffff;
    overflow: hidden;
    padding: 16px 20px 20px 20px;
`;

const Header = styled.div`
    display: flex;
    align-items: center;
    justify-content: space-between;
    margin-bottom: 20px;
`;

const Title = styled.div`
    font-weight: 600;
    font-size: 14px;
    color: ${ANTD_GRAY[7]};
`;

const Icon = styled(SafetyOutlined)`
    margin-right: 8px;
    color: green;
    font-size: 16px;
`;

const Section = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 8px;
`;

// todo: support group ownership reports.
// todo: support subscribed entity reports.
export const HealthSummary = () => {
    const { summary: assertionSummary, loading: assetAssertionSummaryLoading } = useGetOwnedAssetAssertionSummary();
    const { summary: incidentSummary, loading: assetIncidentSummaryLoading } = useGetOwnedAssetIncidentSummary();

    if (!assertionSummary && !incidentSummary) {
        // Do we want a "your assets are healthy" rendering when things are good?
        return null;
    }

    return (
        <Card>
            <Header>
                <Title>
                    <Icon /> Your data health
                </Title>
            </Header>
            <Section>
                {(assertionSummary && (
                    <AssertionSummary summary={assertionSummary} loading={assetAssertionSummaryLoading} />
                )) ||
                    null}
                {(incidentSummary && (
                    <IncidentSummary summary={incidentSummary} loading={assetIncidentSummaryLoading} />
                )) ||
                    null}
            </Section>
        </Card>
    );
};
