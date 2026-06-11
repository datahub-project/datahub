import { Radio, Typography } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { StyledSyntaxHighlighter } from '@app/entityV2/shared/StyledSyntaxHighlighter';
import { InfoItem } from '@app/entityV2/shared/components/styled/InfoItem';
import { ViewHeader } from '@app/entityV2/shared/containers/profile/sidebar/SidebarLogicSection';
import CopyQuery from '@app/entityV2/shared/tabs/Dataset/Queries/CopyQuery';
import { DBT_URN } from '@app/ingest/source/builder/constants';

import { GetDatasetQuery } from '@graphql/dataset.generated';

const JUSTIFY_CONTENT_LEFT = 'left';
const DEFAULT_SYNTAX_LANGUAGE = 'sql';

const InfoSection = styled.div`
    border-bottom: 1px solid ${(props) => props.theme.colors.border};
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

const FormattingSelector = styled.div``;

/**
 * NOTE: To ensure consistent font-family for pre and code tags within as the parent wrapper was overriding it,
 * we explicitly apply 'Roboto Mono', monospace as the font-family for code children using span.
 */
const QueryText = styled(Typography.Paragraph)`
    margin-top: 20px;
    background-color: ${(props) => props.theme.colors.bgSurface};
    span {
        font-family: 'Roboto Mono', monospace !important;
    }
`;

// NOTE: Yes, using `!important` is a shame. However, the SyntaxHighlighter is applying styles directly
// to the component, so there's no way around this
const NestedSyntax = styled(StyledSyntaxHighlighter)`
    background-color: transparent !important;
    border: none !important;
`;

interface ViewTabProps {
    formatOptions: string[];
    showFormatted: boolean;
    setShowFormatted: (showFormatted: boolean) => void;
}

export function ViewTab({ formatOptions, showFormatted, setShowFormatted }: ViewTabProps) {
    return (
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
    );
}

export default function ViewDefinitionTab() {
    const { t } = useTranslation('entity.profile.view');
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const logic = baseEntity?.dataset?.viewProperties?.logic || 'UNKNOWN';
    const formattedLogic = baseEntity?.dataset?.viewProperties?.formattedLogic;

    const materialized = (baseEntity?.dataset?.viewProperties?.materialized && true) || false;
    const language = baseEntity?.dataset?.viewProperties?.language || 'UNKNOWN';
    const canShowFormatted = !!formattedLogic;

    const isDbt = baseEntity?.dataset?.platform?.urn === DBT_URN;
    const formatOptions = isDbt
        ? [t('viewDefinitionTab.formatSource'), t('viewDefinitionTab.formatCompiled')]
        : [t('viewDefinitionTab.formatRaw'), t('viewDefinitionTab.formatFormatted')];
    const [showFormatted, setShowFormatted] = useState(false);

    return (
        <>
            <InfoSection>
                <Typography.Title level={5}>{t('viewDefinitionTab.detailsHeading')}</Typography.Title>
                <InfoItemContainer justifyContent={JUSTIFY_CONTENT_LEFT}>
                    <InfoItem title={t('viewDefinitionTab.materializedLabel')}>
                        <InfoItemContent>
                            {materialized
                                ? t('viewDefinitionTab.materializedTrue')
                                : t('viewDefinitionTab.materializedFalse')}
                        </InfoItemContent>
                    </InfoItem>
                    <InfoItem title={t('viewDefinitionTab.languageLabel')}>
                        <InfoItemContent>{language.toUpperCase()}</InfoItemContent>
                    </InfoItem>
                </InfoItemContainer>
            </InfoSection>
            <InfoSection>
                <Typography.Title level={5}>{t('viewDefinitionTab.logicHeading')}</Typography.Title>
                <ViewHeader>
                    {canShowFormatted && (
                        <ViewTab
                            formatOptions={formatOptions}
                            setShowFormatted={setShowFormatted}
                            showFormatted={showFormatted}
                        />
                    )}
                    <CopyQuery query={showFormatted ? formattedLogic || '' : logic} showCopyText />
                </ViewHeader>
                <QueryText>
                    <NestedSyntax language={language?.toLowerCase() ?? DEFAULT_SYNTAX_LANGUAGE}>
                        {showFormatted ? formattedLogic : logic}
                    </NestedSyntax>
                </QueryText>
            </InfoSection>
        </>
    );
}
