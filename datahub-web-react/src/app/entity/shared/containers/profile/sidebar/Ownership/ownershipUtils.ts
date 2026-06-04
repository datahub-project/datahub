import i18next from 'i18next';

import { OwnershipType } from '@types';

/**
 * Builds a mapping from OwnershipType to its translated display-name & description.
 * Called at use time (not import time) so i18n is already initialized.
 */
const getOwnershipTypeDetails = (): Map<OwnershipType, { name: string; description: string }> =>
    new Map([
        [
            OwnershipType.TechnicalOwner,
            {
                name: i18next.t('entityV1.shared.containers:ownershipType.technicalOwner'),
                description: i18next.t('entityV1.shared.containers:ownershipType.technicalOwnerDescription'),
            },
        ],
        [
            OwnershipType.BusinessOwner,
            {
                name: i18next.t('entityV1.shared.containers:ownershipType.businessOwner'),
                description: i18next.t('entityV1.shared.containers:ownershipType.businessOwnerDescription'),
            },
        ],
        [
            OwnershipType.DataSteward,
            {
                name: i18next.t('entityV1.shared.containers:ownershipType.dataSteward'),
                description: i18next.t('entityV1.shared.containers:ownershipType.dataStewardDescription'),
            },
        ],
        [
            OwnershipType.None,
            {
                name: i18next.t('common.labels:none'),
                description: i18next.t('entityV1.shared.containers:ownershipType.noneDescription'),
            },
        ],
    ]);

export const getNameFromType = (type: OwnershipType) => {
    return getOwnershipTypeDetails().get(type)?.name || type;
};

export const getDescriptionFromType = (type: OwnershipType) => {
    return (
        getOwnershipTypeDetails().get(type)?.description ||
        i18next.t('entityV1.shared.containers:ownershipType.noDescription')
    );
};
