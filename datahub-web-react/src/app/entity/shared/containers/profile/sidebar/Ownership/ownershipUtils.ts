import { OwnershipType } from '../../../../../../../types.generated';

/**
 * A mapping from OwnershipType to it's display name & description. In the future,
 * we intend to make this configurable.
 */
export const OWNERSHIP_DISPLAY_TYPES = [
    {
        type: OwnershipType.TechnicalOwner,
        name: 'Technical Owner',
        description: 'Involved in the production, maintenance, or distribution of the asset(s).',
    },
    {
        type: OwnershipType.BusinessOwner,
        name: 'Business Owner',
        description: 'Principle stakeholders or domain experts associated with the asset(s).',
    },
    {
        type: OwnershipType.DataSteward,
        name: 'Data Steward',
        description: 'Involved in governance of the asset(s).',
    },
    {
        type: OwnershipType.None,
        name: 'None',
        description: 'No ownership type specified.',
    },
];

const ownershipTypeToDetails = new Map();
OWNERSHIP_DISPLAY_TYPES.forEach((ownershipDetails) => {
    ownershipTypeToDetails.set(ownershipDetails.type, ownershipDetails);
});

export const getNameFromType = (type: OwnershipType) => {
    return ownershipTypeToDetails.get(type)?.name || type;
};

export const getDescriptionFromType = (type: OwnershipType) => {
    return ownershipTypeToDetails.get(type)?.description || 'No description';
};
