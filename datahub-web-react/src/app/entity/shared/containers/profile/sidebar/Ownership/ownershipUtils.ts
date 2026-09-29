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
                name: i18next.t('entity.shared.containers:sidebar.ownership.type.technicalOwnerName'),
                description: i18next.t('entity.shared.containers:sidebar.ownership.type.technicalOwnerDescription'),
            },
        ],
        [
            OwnershipType.BusinessOwner,
            {
                name: i18next.t('entity.shared.containers:sidebar.ownership.type.businessOwnerName'),
                description: i18next.t('entity.shared.containers:sidebar.ownership.type.businessOwnerDescription'),
            },
        ],
        [
            OwnershipType.DataSteward,
            {
                name: i18next.t('entity.shared.containers:sidebar.ownership.type.dataStewardName'),
                description: i18next.t('entity.shared.containers:sidebar.ownership.type.dataStewardDescription'),
            },
        ],
        [
            OwnershipType.None,
            {
                name: i18next.t('common.labels:none'),
                description: i18next.t('entity.shared.containers:sidebar.ownership.type.noneDescription'),
            },
        ],
    ]);

export const getNameFromType = (type: OwnershipType) => {
    return getOwnershipTypeDetails().get(type)?.name || type;
};

export const getDescriptionFromType = (type: OwnershipType) => {
    return (
        getOwnershipTypeDetails().get(type)?.description ||
        i18next.t('entity.shared.containers:ownershipType.noDescription')
    );
};
