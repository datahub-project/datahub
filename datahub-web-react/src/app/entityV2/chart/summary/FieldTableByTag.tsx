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
