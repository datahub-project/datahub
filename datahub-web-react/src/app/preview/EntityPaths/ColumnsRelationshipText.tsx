import { Maybe } from 'graphql/jsutils/Maybe';
import React, { useContext } from 'react';
import styled from 'styled-components/macro';

import { downgradeV2FieldPath } from '@app/entity/dataset/profile/schema/utils/utils';
import { LineageTabContext } from '@app/entity/shared/tabs/Lineage/LineageTabContext';
import { decodeSchemaField } from '@app/lineage/utils/columnLineageUtils';
import DisplayedColumns from '@app/preview/EntityPaths/DisplayedColumns';

import { Entity, LineageDirection } from '@types';

const ColumnNameWrapper = styled.span<{ isBlack?: boolean }>`
    font-family: 'Roboto Mono', monospace;
    font-weight: bold;
    ${(props) => props.isBlack && 'color: black;'}
`;

interface Props {
    displayedColumns: (Maybe<Entity> | undefined)[];
}

export default function ColumnsRelationshipText({ displayedColumns }: Props) {
    const { selectedColumn, lineageDirection } = useContext(LineageTabContext);

    const displayedFieldPath = decodeSchemaField(downgradeV2FieldPath(selectedColumn) || '');

    return (
        <>
            {lineageDirection === LineageDirection.Downstream ? (
                <span>
                    <ColumnNameWrapper>{displayedFieldPath}</ColumnNameWrapper> to&nbsp;
                    <DisplayedColumns displayedColumns={displayedColumns} />
                </span>
            ) : (
                <span>
                    <DisplayedColumns displayedColumns={displayedColumns} /> to{' '}
                    <ColumnNameWrapper>{displayedFieldPath}</ColumnNameWrapper>
                </span>
            )}
        </>
    );
}
