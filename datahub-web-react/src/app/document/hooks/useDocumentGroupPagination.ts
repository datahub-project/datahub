import { useState } from 'react';

import {
    DocumentGroupPaginationState,
    createDocumentGroupPaginationState,
    getDocumentGroupPaginationView,
} from '@app/document/utils/documentSidebarGroupPagination';

type UrnEntity = {
    urn: string;
};

/**
 * Keeps domain-group scroll state aligned with the current query criteria.
 * Reset happens during render so A → B → A cannot reuse A's old offset/rows.
 */
export default function useDocumentGroupPagination<T extends UrnEntity>(criteriaKey: string) {
    const [pagination, setPagination] = useState(() => createDocumentGroupPaginationState<T>(criteriaKey));
    if (pagination.criteriaKey !== criteriaKey) {
        setPagination(createDocumentGroupPaginationState<T>(criteriaKey));
    }

    return {
        ...getDocumentGroupPaginationView(pagination, criteriaKey),
        setPagination,
    };
}

export type { DocumentGroupPaginationState };
