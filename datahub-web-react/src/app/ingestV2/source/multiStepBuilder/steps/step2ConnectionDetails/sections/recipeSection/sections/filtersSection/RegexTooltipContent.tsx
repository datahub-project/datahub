import { Text } from '@components';
import React from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components';

const ContentContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 8px;
    padding: 8px;
`;

const EXAMPLE_EXACT = 'public.customers';
const EXAMPLE_PATTERN = 'public.sales_.*';
const EXAMPLE_MULTIPLE = 'public.orders, analytics.*';

export default function RegexTooltipContent() {
    const { t } = useTranslation('ingestion.sourceBuilder');
    return (
        <ContentContainer>
            <Text>{t('multiStep.filters.tooltip.intro')}</Text>
            <Text weight="semiBold"> {t('multiStep.filters.tooltip.examples')}</Text>
            <Text>
                <ul>
                    <li>
                        <Trans t={t} i18nKey="multiStep.filters.tooltip.exact" values={{ example: EXAMPLE_EXACT }} />
                    </li>
                    <li>
                        <Trans
                            t={t}
                            i18nKey="multiStep.filters.tooltip.pattern"
                            values={{ example: EXAMPLE_PATTERN }}
                        />
                    </li>
                    <li>
                        <Trans
                            t={t}
                            i18nKey="multiStep.filters.tooltip.multiple"
                            values={{ example: EXAMPLE_MULTIPLE }}
                        />
                    </li>
                </ul>
                {t('multiStep.filters.tooltip.separator')}
            </Text>
        </ContentContainer>
    );
}
