import { Typography } from 'antd';
import React from 'react';
import styled from 'styled-components';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { ANTD_GRAY } from '../../../constants';
import { useBaseEntity } from '../../../EntityContext';
import { InfoItem } from '../../../components/styled/InfoItem';

const InfoSection = styled.div`
    border-bottom: 1px solid ${ANTD_GRAY[4.5]};
    padding: 16px 20px;
`;

const InfoItemContainer = styled.div<{ justifyContent }>`
    display: flex;
    position: relative;
    z-index: 1;
    justify-content: ${(props) => props.justifyContent};
    padding: 12px 2px;
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

export default function ViewDefinitionTab() {
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const logic = baseEntity?.dataset?.viewProperties?.logic || 'UNKNOWN';
    const materialized = (baseEntity?.dataset?.viewProperties?.materialized && true) || false;
    const language = baseEntity?.dataset?.viewProperties?.language || 'UNKNOWN';

    return (
        <>
            <InfoSection>
                <Typography.Title level={5}>Details</Typography.Title>
                <InfoItemContainer justifyContent="left">
                    <InfoItem title="Materialized">
                        <div style={{ paddingTop: 8 }}>{materialized ? 'True' : 'False'}</div>
                    </InfoItem>
                    <InfoItem title="Language">
                        <div style={{ paddingTop: 8 }}>{language.toUpperCase()}</div>
                    </InfoItem>
                </InfoItemContainer>
            </InfoSection>
            <InfoSection>
                <Typography.Title level={5}>Logic</Typography.Title>
                <QueryText>
                    <NestedSyntax language="sql">{logic}</NestedSyntax>
                </QueryText>
            </InfoSection>
        </>
    );
}
