import React from 'react';
import { useTranslation } from 'react-i18next';
import styled from 'styled-components';

const Pill = styled.div`
    background-color: ${(props) => props.theme.colors.bg};
    border-radius: 10px;
    border: 1px solid;
    font-size: 12px;
    font-weight: 400;

    padding: 0 8px;
`;

const PrimaryKeyPill = styled(Pill)`
    color: ${(props) => props.theme.colors.textBrand} !important;
    border-color: ${(props) => props.theme.colors.borderBrand};
`;

const ForeignKeyPill = styled(Pill)`
    color: ${(props) => props.theme.colors.textSuccess} !important;
    border-color: ${(props) => props.theme.colors.borderSuccess};
`;

const NullablePill = styled(Pill)`
    color: ${(props) => props.theme.colors.textTertiary} !important;
    border-color: ${(props) => props.theme.colors.border};
`;

export function PrimaryKeyLabel() {
    const { t } = useTranslation('entity.profile.schema');
    return <PrimaryKeyPill>{t('constraintLabels.primaryKey')}</PrimaryKeyPill>;
}

export function ForeignKeyLabel() {
    const { t } = useTranslation('entity.profile.schema');
    return <ForeignKeyPill>{t('constraintLabels.foreignKey')}</ForeignKeyPill>;
}

export function PartitioningKeyLabel() {
    const { t } = useTranslation('entity.profile.schema');
    return <PrimaryKeyPill>{t('constraintLabels.partitionKey')}</PrimaryKeyPill>;
}

export default function NullableLabel() {
    const { t } = useTranslation('entity.profile.schema');
    return <NullablePill>{t('constraintLabels.nullable')}</NullablePill>;
}
