import { Maybe } from 'graphql/jsutils/Maybe';
import React, { useContext } from 'react';
import { Trans, useTranslation } from 'react-i18next';
import styled from 'styled-components/macro';

import { downgradeV2FieldPath } from '@app/entity/dataset/profile/schema/utils/utils';
import { LineageTabContext } from '@app/entityV2/shared/tabs/Lineage/LineageTabContext';
import { decodeSchemaField } from '@app/lineage/utils/columnLineageUtils';
import DisplayedColumns from '@app/previewV2/EntityPaths/DisplayedColumns';

import { Entity, LineageDirection } from '@types';

const ColumnNameWrapper = styled.span<{ isBlack?: boolean }>`
    font-weight: bold;
    ${(props) => props.isBlack && `color: ${props.theme.colors.text};`}
`;

interface Props {
    displayedColumns: (Maybe<Entity> | undefined)[];
}

export default function ColumnsRelationshipText({ displayedColumns }: Props) {
    const { t } = useTranslation('entity.preview');
    const { selectedColumn, lineageDirection } = useContext(LineageTabContext);

    const displayedFieldPath = decodeSchemaField(downgradeV2FieldPath(selectedColumn) || '');

    return (
        <span>
            <Trans
                t={t}
                i18nKey={
                    lineageDirection === LineageDirection.Downstream
                        ? 'columnRelationship.downstream'
                        : 'columnRelationship.upstream'
                }
                values={{ fieldPath: displayedFieldPath }}
                components={{
                    field: <ColumnNameWrapper />,
                    columns: <DisplayedColumns displayedColumns={displayedColumns} />,
                }}
            />
        </span>
    );
}
