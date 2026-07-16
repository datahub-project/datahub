import { EXACT_SEARCH_PREFIX, SEARCH_FOR_ENTITY_PREFIX } from '@app/searchV2/utils/constants';

export default function filterSearchQuery(v: string) {
    return (v && v.startsWith(SEARCH_FOR_ENTITY_PREFIX)) || v.startsWith(EXACT_SEARCH_PREFIX) ? v.split('__')[1] : v;
}
