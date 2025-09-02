import { AssetProperty, PropertyType } from '@app/entityV2/summary/properties/types';
import { DATE_TYPE_URN, NUMBER_TYPE_URN, STRING_TYPE_URN, URN_TYPE_URN } from '@app/shared/constants';

export const OWNERS_PROPERTY: AssetProperty = {
    name: 'Owners',
    type: PropertyType.Owners,
    icon: 'UserCircle',
};

export const DOMAIN_PROPERTY: AssetProperty = {
    name: 'Domain',
    type: PropertyType.Domain,
    icon: 'Globe',
};

export const TAGS_PROPERTY: AssetProperty = {
    name: 'Tags',
    type: PropertyType.Tags,
    icon: 'Tag',
};

export const TERMS_PROPERTY: AssetProperty = {
    name: 'Terms',
    type: PropertyType.Terms,
    icon: 'BookmarkSimple',
};

export const CREATED_PROPERTY: AssetProperty = {
    name: 'Created',
    type: PropertyType.Created,
    icon: 'Clock',
};

export const LAST_UPDATED_PROPERTY: AssetProperty = {
    name: 'Last Updated',
    type: PropertyType.LastUpdated,
    icon: 'Clock',
};

export const VERIFICATION_STATUS_PROPERTY: AssetProperty = {
    name: 'Verification Status',
    type: PropertyType.VerificationStatus,
    icon: 'SealCheck',
};

export const SUPPORTED_STRUCTURED_PROPERTY_VALUE_TYPES = [
    STRING_TYPE_URN,
    NUMBER_TYPE_URN,
    URN_TYPE_URN,
    DATE_TYPE_URN,
];
