import { SORT_OPTIONS } from '@app/searchV2/context/constants';

export default function useGetSortOptions() {
    // TODO: Add a new endpoint showSortFields() that passes the list of potential sort fields, and verifies
    // whether there are any entries matching that sort field.
    return SORT_OPTIONS;
}
