import React from 'react';
import { useTranslation } from 'react-i18next';

import { useEntityRegistry } from '@app/useEntityRegistry';

import { Owner } from '@types';

interface Props {
    owners: Owner[];
}

export default function FormRequestedBy({ owners }: Props) {
    const { t } = useTranslation('entity.form');
    const entityRegistry = useEntityRegistry();

    const ownerNames = owners.map((ownerAssoc) =>
        entityRegistry.getDisplayName(ownerAssoc.owner.type, ownerAssoc.owner),
    );

    return <>{t('requestedBy', { owners: ownerNames })}</>;
}
