import { BookmarkSimple } from '@phosphor-icons/react/dist/csr/BookmarkSimple';
import { Clock } from '@phosphor-icons/react/dist/csr/Clock';
import { Globe } from '@phosphor-icons/react/dist/csr/Globe';
import { Tag } from '@phosphor-icons/react/dist/csr/Tag';
import { UserCircle } from '@phosphor-icons/react/dist/csr/UserCircle';
import i18next from 'i18next';

import { AssetProperty } from '@app/entityV2/summary/properties/types';
import { DATE_TYPE_URN, NUMBER_TYPE_URN, STRING_TYPE_URN, URN_TYPE_URN } from '@app/shared/constants';

import { SummaryElementType } from '@types';

export const OWNERS_PROPERTY: AssetProperty = {
    get name() {
        return i18next.t('common.labels:owners');
    },
    type: SummaryElementType.Owners,
    icon: UserCircle,
};

export const DOMAIN_PROPERTY: AssetProperty = {
    get name() {
        return i18next.t('common.labels:domain');
    },
    type: SummaryElementType.Domain,
    icon: Globe,
};

export const TAGS_PROPERTY: AssetProperty = {
    get name() {
        return i18next.t('common.labels:tags');
    },
    type: SummaryElementType.Tags,
    icon: Tag,
};

export const TERMS_PROPERTY: AssetProperty = {
    get name() {
        return i18next.t('entity.profile.summary:properties.terms');
    },
    type: SummaryElementType.GlossaryTerms,
    icon: BookmarkSimple,
};

export const CREATED_PROPERTY: AssetProperty = {
    get name() {
        return i18next.t('common.labels:created');
    },
    type: SummaryElementType.Created,
    icon: Clock,
};

export const SUPPORTED_STRUCTURED_PROPERTY_VALUE_TYPES = [
    STRING_TYPE_URN,
    NUMBER_TYPE_URN,
    URN_TYPE_URN,
    DATE_TYPE_URN,
];
