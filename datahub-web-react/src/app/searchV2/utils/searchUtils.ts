import { MAX_COUNT_VAL } from '@app/searchV2/utils/constants';

// we can't ask for more than the max (10,000) so calculate count to ask for up to but no more than the max
export function getSearchCount(start: number, numResultsPerPage: number) {
    let count = numResultsPerPage;
    if (start + numResultsPerPage > MAX_COUNT_VAL) {
        count = MAX_COUNT_VAL - start;
    }
    return count;
}
