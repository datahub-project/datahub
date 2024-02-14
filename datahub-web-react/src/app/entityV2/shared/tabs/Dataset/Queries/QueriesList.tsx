import React from 'react';
import styled from 'styled-components';
import Query from './Query';
import { Query as QueryType } from './types';

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
