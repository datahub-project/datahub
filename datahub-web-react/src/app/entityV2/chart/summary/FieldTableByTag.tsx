import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import ChartFieldsTable from '@app/entityV2/chart/summary/ChartFieldsTable';
import { SummaryTabHeaderTitle } from '@app/entityV2/shared/summary/HeaderComponents';

import { SchemaField } from '@types';

const ColumnWrapper = styled.div`
    display: flex;
    flex-direction: column;
`;

interface Props {
    title: string;
    fields: SchemaField[];
}

export default function FieldTableByTag({ title, fields }: Props) {
    const { urn } = useEntityData();

    if (!fields?.length) {
        return null;
    }

    return (
        <>
            <ColumnWrapper>
                <SummaryTabHeaderTitle title={title} />
                <ChartFieldsTable urn={urn} rows={fields} />
            </ColumnWrapper>
        </>
    );
}
