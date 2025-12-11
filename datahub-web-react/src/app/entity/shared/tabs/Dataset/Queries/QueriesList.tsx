/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import Query from '@app/entity/shared/tabs/Dataset/Queries/Query';
import { Query as QueryType } from '@app/entity/shared/tabs/Dataset/Queries/types';

const List = styled.div`
    margin-bottom: 28px;
    display: flex;
    align-items: center;
    justify-content: left;
    flex-wrap: wrap;
`;

type Props = {
    queries: QueryType[];
    showDetails?: boolean;
    showEdit?: boolean;
    showDelete?: boolean;
    onDeleted?: (query) => void;
    onEdited?: (newQuery) => void;
};

export default function QueriesList({
    queries,
    showEdit = true,
    showDelete = true,
    showDetails = true,
    onDeleted,
    onEdited,
}: Props) {
    return (
        <List>
            {queries.map((query, idx) => (
                <Query
                    urn={query.urn}
                    title={query.title || undefined}
                    description={query.description || undefined}
                    query={query.query}
                    createdAtMs={query.createdTime}
                    showDelete={showDelete}
                    showEdit={showEdit}
                    showDetails={showDetails}
                    onDeleted={() => onDeleted?.(query)}
                    onEdited={(newQuery) => onEdited?.(newQuery)}
                    index={idx}
                />
            ))}
        </List>
    );
}
