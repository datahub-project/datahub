import { LoadingOutlined } from '@ant-design/icons';
import { Spin } from 'antd';
import React from 'react';
import { useTranslation } from 'react-i18next';

import { HelperText, ResultContainer } from '@app/context/import/components/importDocumentsModal.styles';
import { Text } from '@src/alchemy-components';

export default function ImportDocumentsImportingStep() {
    const { t } = useTranslation('misc');

    return (
        <ResultContainer>
            <Spin indicator={<LoadingOutlined style={{ fontSize: 36 }} spin />} />
            <Text weight="semiBold">{t('context.import.importingTitle')}</Text>
            <HelperText>{t('context.import.importingSubtitle')}</HelperText>
        </ResultContainer>
    );
}
