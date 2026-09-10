import React from 'react';
import { useTranslation } from 'react-i18next';

import { HelperText, ResultContainer } from '@app/context/import/components/importDocumentsModal.styles';
import { Loader, Text } from '@src/alchemy-components';

export default function ImportDocumentsImportingStep() {
    const { t } = useTranslation('misc');

    return (
        <ResultContainer>
            <Loader size="md" />
            <Text weight="semiBold">{t('context.import.importingTitle')}</Text>
            <HelperText>{t('context.import.importingSubtitle')}</HelperText>
        </ResultContainer>
    );
}
