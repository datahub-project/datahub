import { SimpleSelect } from '@components';
import React, { useMemo } from 'react';
import { useTranslation } from 'react-i18next';

import { OwnershipTypeEntity } from '@src/types.generated';

interface Props {
    selectedOwnerTypeUrn?: string;
    ownershipTypes: OwnershipTypeEntity[];
    onSelectOwnerType: (typeUrn: string) => void;
}

export default function OwnershipTypesSelect({ selectedOwnerTypeUrn, ownershipTypes, onSelectOwnerType }: Props) {
    const { t } = useTranslation('entity.shared.containers');
    const options = useMemo(
        () =>
            ownershipTypes.map((ownershipType: OwnershipTypeEntity | undefined) => ({
                value: ownershipType?.urn || '',
                label: ownershipType?.info?.name || ownershipType?.urn || '',
                description: ownershipType?.info?.description || '',
            })),
        [ownershipTypes],
    );

    return (
        <SimpleSelect
            options={options}
            values={selectedOwnerTypeUrn ? [selectedOwnerTypeUrn] : []}
            onUpdate={(values) => {
                if (values[0]) onSelectOwnerType(values[0]);
            }}
            placeholder={t('sidebar.ownership.selectTypePlaceholder')}
            showClear={false}
            width="full"
        />
    );
}
