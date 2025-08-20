import { AssetProperty, PropertyType } from '@app/entityV2/summary/properties/types';

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

export const ASSET_PROPERTIES_MAPPING: ReadonlyMap<PropertyType, AssetProperty> = new Map([
    [PropertyType.Created, CREATED_PROPERTY],
    [PropertyType.Domain, DOMAIN_PROPERTY],
    [PropertyType.Owners, OWNERS_PROPERTY],
    [PropertyType.Tags, TAGS_PROPERTY],
    [PropertyType.Terms, TERMS_PROPERTY],
    [PropertyType.LastUpdated, LAST_UPDATED_PROPERTY],
    [PropertyType.VerificationStatus, VERIFICATION_STATUS_PROPERTY],
]);
