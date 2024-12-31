import { Radio, Typography } from 'antd';
import React, { useState } from 'react';
import styled from 'styled-components';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { GetDatasetQuery } from '../../../../../../graphql/dataset.generated';
import { ANTD_GRAY } from '../../../constants';
import { useBaseEntity } from '../../../EntityContext';
import { InfoItem } from '../../../components/styled/InfoItem';
import { DBT_URN } from '../../../../../ingest/source/builder/constants';

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

const FormattingSelector = styled.div`
    margin-top: 10px;
`;

const QueryText = styled(Typography.Paragraph)`
    margin-top: 15px;
    background-color: ${ANTD_GRAY[2]};
    border-radius: 5px;
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
    const formattedLogic = baseEntity?.dataset?.viewProperties?.formattedLogic;
    const materialized = (baseEntity?.dataset?.viewProperties?.materialized && true) || false;
    const language = baseEntity?.dataset?.viewProperties?.language || 'UNKNOWN';

    const isDbt = baseEntity?.dataset?.platform?.urn === DBT_URN;
    const formatOptions = isDbt ? ['Source', 'Compiled'] : ['Raw', 'Formatted'];

    const canShowFormatted = !!formattedLogic;
    const [showFormatted, setShowFormatted] = useState(false);

    return (
        <>
            <InfoSection>
                <Typography.Title level={5}>Details</Typography.Title>
                <InfoItemContainer justifyContent="left">
                    <InfoItem title="Materialized">
                        <InfoItemContent>{materialized ? 'True' : 'False'}</InfoItemContent>
                    </InfoItem>
                    <InfoItem title="Language">
                        <InfoItemContent>{language.toUpperCase()}</InfoItemContent>
                    </InfoItem>
                </InfoItemContainer>
            </InfoSection>
            <InfoSection>
                <Typography.Title level={5}>Logic</Typography.Title>
                {canShowFormatted && (
                    <FormattingSelector>
                        <Radio.Group
                            options={[
                                { label: formatOptions[0], value: false },
                                { label: formatOptions[1], value: true },
                            ]}
                            onChange={(e) => setShowFormatted(e.target.value)}
                            value={showFormatted}
                            optionType="button"
                        />
                    </FormattingSelector>
                )}
                <QueryText>
                    <NestedSyntax language="sql">{showFormatted ? formattedLogic : logic}</NestedSyntax>
                </QueryText>
            </InfoSection>
        </>
    );
}
