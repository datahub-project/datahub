import i18next from 'i18next';

import { forcePluralize } from '@app/shared/textUtil';

import { OwnershipType, OwnershipTypeEntity } from '@types';

/**
 * A mapping from OwnershipType to it's display name & description. In the future,
 * we intend to make this configurable.
 */
const OWNERSHIP_DISPLAY_TYPES = [
    {
        type: OwnershipType.TechnicalOwner,
        get name() {
            return i18next.t('entity.shared.containers:sidebar.ownership.type.technicalOwnerName');
        },
        get description() {
            return i18next.t('entity.shared.containers:sidebar.ownership.type.technicalOwnerDescription');
        },
    },
    {
        type: OwnershipType.BusinessOwner,
        get name() {
            return i18next.t('entity.shared.containers:sidebar.ownership.type.businessOwnerName');
        },
        get description() {
            return i18next.t('entity.shared.containers:sidebar.ownership.type.businessOwnerDescription');
        },
    },
    {
        type: OwnershipType.DataSteward,
        get name() {
            return i18next.t('entity.shared.containers:sidebar.ownership.type.dataStewardName');
        },
        get description() {
            return i18next.t('entity.shared.containers:sidebar.ownership.type.dataStewardDescription');
        },
    },
    {
        type: OwnershipType.None,
        get name() {
            return i18next.t('entity.shared.containers:sidebar.ownership.type.noneName');
        },
        get description() {
            return i18next.t('entity.shared.containers:sidebar.ownership.type.noneDescription');
        },
    },
];

const ownershipTypeToDetails = new Map();
OWNERSHIP_DISPLAY_TYPES.forEach((ownershipDetails) => {
    ownershipTypeToDetails.set(ownershipDetails.type, ownershipDetails);
});

export const getNameFromType = (type: OwnershipType) => {
    return ownershipTypeToDetails.get(type)?.name || type;
};

export function getOwnershipTypeName(ownershipType?: OwnershipTypeEntity | null) {
    return (
        (ownershipType?.info?.name && forcePluralize(ownershipType?.info?.name)) ||
        i18next.t('entity.shared.containers:sidebar.ownership.type.otherName')
    );
}
