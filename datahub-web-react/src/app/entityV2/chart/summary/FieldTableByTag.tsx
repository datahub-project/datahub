import React from 'react';
import styled from 'styled-components';
import ChartFieldsTable from './ChartFieldsTable';
import { SchemaField } from '../../../../types.generated';
import { SummaryTabHeaderTitle } from '../../shared/summary/HeaderComponents';
import { useEntityData } from '../../../entity/shared/EntityContext';

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
