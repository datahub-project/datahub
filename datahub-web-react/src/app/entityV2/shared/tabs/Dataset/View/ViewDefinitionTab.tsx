import { CodeBlock } from '@components';
import { Typography } from 'antd';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { InfoItem } from '@app/entityV2/shared/components/styled/InfoItem';
import { DBT_URN } from '@app/ingest/source/builder/constants';

import { GetDatasetQuery } from '@graphql/dataset.generated';

const JUSTIFY_CONTENT_LEFT = 'left';
const DEFAULT_SYNTAX_LANGUAGE = 'sql';
const SOURCE_OPTION = 'source';
const FORMATTED_OPTION = 'formatted';

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

const CodeBlockWrapper = styled.div`
    margin-top: 20px;
`;

export default function ViewDefinitionTab() {
    const { t } = useTranslation('entity.profile.view');
    const baseEntity = useBaseEntity<GetDatasetQuery>();
    const logic = baseEntity?.dataset?.viewProperties?.logic || 'UNKNOWN';
    const formattedLogic = baseEntity?.dataset?.viewProperties?.formattedLogic;

    const materialized = (baseEntity?.dataset?.viewProperties?.materialized && true) || false;
    const language = baseEntity?.dataset?.viewProperties?.language || 'UNKNOWN';
    const canShowFormatted = !!formattedLogic;

    const isDbt = baseEntity?.dataset?.platform?.urn === DBT_URN;
    const [showFormatted, setShowFormatted] = useState(false);
    const languageOptions = canShowFormatted
        ? [
              {
                  label: isDbt ? t('viewDefinitionTab.formatSource') : t('viewDefinitionTab.formatRaw'),
                  value: SOURCE_OPTION,
              },
              {
                  label: isDbt ? t('viewDefinitionTab.formatCompiled') : t('viewDefinitionTab.formatFormatted'),
                  value: FORMATTED_OPTION,
              },
          ]
        : undefined;
    const selectedLanguage = showFormatted ? FORMATTED_OPTION : SOURCE_OPTION;
    const code = showFormatted ? formattedLogic || logic : logic;

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
                <CodeBlockWrapper>
                    <CodeBlock
                        code={code}
                        language={language?.toLowerCase() ?? DEFAULT_SYNTAX_LANGUAGE}
                        languageLabel={false}
                        languageOptions={languageOptions}
                        selectedLanguage={selectedLanguage}
                        onLanguageChange={(value) => setShowFormatted(value === FORMATTED_OPTION)}
                    />
                </CodeBlockWrapper>
            </InfoSection>
        </>
    );
}
