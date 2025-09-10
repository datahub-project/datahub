import { AssetProperty } from '@app/entityV2/summary/properties/types';
import { DATE_TYPE_URN, NUMBER_TYPE_URN, STRING_TYPE_URN, URN_TYPE_URN } from '@app/shared/constants';

import { SummaryElementType } from '@types';

export const OWNERS_PROPERTY: AssetProperty = {
    name: 'Owners',
    type: SummaryElementType.Owners,
    icon: 'UserCircle',
};

export const DOMAIN_PROPERTY: AssetProperty = {
    name: 'Domain',
    type: SummaryElementType.Domain,
    icon: 'Globe',
};

export const TAGS_PROPERTY: AssetProperty = {
    name: 'Tags',
    type: SummaryElementType.Tags,
    icon: 'Tag',
};

export const TERMS_PROPERTY: AssetProperty = {
    name: 'Terms',
    type: SummaryElementType.GlossaryTerms,
    icon: 'BookmarkSimple',
};

export const CREATED_PROPERTY: AssetProperty = {
    name: 'Created',
    type: SummaryElementType.Created,
    icon: 'Clock',
};

export const SUPPORTED_STRUCTURED_PROPERTY_VALUE_TYPES = [
    STRING_TYPE_URN,
    NUMBER_TYPE_URN,
    URN_TYPE_URN,
    DATE_TYPE_URN,
];
