import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { GetChartQuery } from '../../../graphql/chart.generated';
import { ANTD_GRAY } from '../shared/constants';
import { useBaseEntity } from '../shared/EntityContext';
import { InfoItem } from '../shared/components/styled/InfoItem';

const InfoSection = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    padding: 16px 20px;
`;

const InfoItemContainer = styled.div<{ justifyContent }>`
    display: flex;
    position: relative;
    justify-content: ${(props) => props.justifyContent};
    padding: 12px 2px;
`;

const InfoItemContent = styled.div`
    padding-top: 8px;
`;

const QueryText = styled(Typography.Paragraph)`
    margin-top: 20px;
    background-color: ${ANTD_GRAY[2]};
`;

// NOTE: Yes, using `!important` is a shame. However, the SyntaxHighlighter is applying styles directly
// to the component, so there's no way around this
const NestedSyntax = styled(SyntaxHighlighter)`
    background-color: transparent !important;
    border: none !important;
`;

export function ChartQueryTab() {
    const baseEntity = useBaseEntity<GetChartQuery>();
    const query = baseEntity?.chart?.query?.rawQuery || 'UNKNOWN';
    const type = baseEntity?.chart?.query?.type || 'UNKNOWN';

    return (
        <>
            <InfoSection>
                <Typography.Title level={5}>Details</Typography.Title>
                <InfoItemContainer justifyContent="left">
                    <InfoItem title="Type">
                        <InfoItemContent>{type.toUpperCase()}</InfoItemContent>
                    </InfoItem>
                </InfoItemContainer>
            </InfoSection>
            <InfoSection>
                <Typography.Title level={5}>Query</Typography.Title>
                <QueryText>
                    <NestedSyntax language="sql">{query}</NestedSyntax>
                </QueryText>
            </InfoSection>
        </>
    );
}
