import { Text } from '@components';
import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

import { EmptyContainer } from '@app/govern/structuredProperties/styledComponents';
import EmptyFormsImage from '@src/images/empty-forms.svg?react';

const TextContainer = styled.div`
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
`;

interface Props {
    sourceType?: string;
    isEmptySearchResult?: boolean;
}

const EmptySources = ({ sourceType, isEmptySearchResult }: Props) => {
    const { t } = useTranslation('ingestion');
    return (
        <EmptyContainer>
            {isEmptySearchResult ? (
                <TextContainer>
                    <Text size="lg" color="gray" weight="bold">
                        {t('source.emptySearchTitle')}
                    </Text>
                    <Text size="sm" color="gray" weight="normal">
                        {t('source.emptySearchSubtitle')}
                    </Text>
                </TextContainer>
            ) : (
                <>
                    <EmptyFormsImage />
                    <Text size="md" color="gray" weight="bold">
                        {t('source.emptyTitle', { sourceType: sourceType || t('source.sourcesNoun') })}
                    </Text>
                </>
            )}
        </EmptyContainer>
    );
};

export default EmptySources;
