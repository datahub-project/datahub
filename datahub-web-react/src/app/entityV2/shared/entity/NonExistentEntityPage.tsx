import { EmptyState } from '@components';
import { FileX } from '@phosphor-icons/react/dist/csr/FileX';
import React from 'react';
import { useTranslation } from 'react-i18next';

export default function NonExistentEntityPage() {
    const { t } = useTranslation('entity.shared.emptyStates');
    return <EmptyState icon={FileX} size="lg" title={t('notFound.title')} description={t('notFound.subtitle')} />;
}
