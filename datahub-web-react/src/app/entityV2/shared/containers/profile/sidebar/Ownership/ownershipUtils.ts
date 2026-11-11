import i18next from 'i18next';

import { OwnershipType, OwnershipTypeEntity } from '@types';

// Built-in ownership types carry URNs under this prefix (e.g. __system__technical_owner).
// Their English display names pluralize with a naive "s"; custom types do not — especially
// in non-English languages where plural rules differ — so custom type names are shown verbatim.
const SYSTEM_OWNERSHIP_TYPE_URN_PREFIX = 'urn:li:ownershipType:__system__';

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
    const name = ownershipType?.info?.name;
    if (!name) {
        return i18next.t('entity.shared.containers:sidebar.ownership.type.otherName');
    }
    // Only the built-in system types get a pluralized label; custom types are shown as-is.
    if (ownershipType?.urn?.startsWith(SYSTEM_OWNERSHIP_TYPE_URN_PREFIX)) {
        return i18next.t('entity.shared.containers:sidebar.ownership.type.pluralName', { name });
    }
    return name;
}
