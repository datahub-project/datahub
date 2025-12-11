/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from '@components';
import React, { useContext } from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { LineageTabContext } from '@app/entityV2/shared/tabs/Lineage/LineageTabContext';
import ColumnsRelationshipText from '@app/previewV2/EntityPaths/ColumnsRelationshipText';
import DisplayedColumns from '@app/previewV2/EntityPaths/DisplayedColumns';

import { EntityPath, EntityType, LineageDirection, SchemaFieldEntity } from '@types';

export const ResultText = styled.span`
    white-space: nowrap;
    &:hover {
        border-bottom: 1px solid black;
        cursor: pointer;
    }
`;

const DescriptionWrapper = styled.span`
    color: ${ANTD_GRAY[8]};
    white-space: nowrap;
`;

export function getDisplayedColumns(paths: EntityPath[], resultEntityUrn: string) {
    return paths
        .map((path) =>
            path.path?.filter(
                (entity) =>
                    entity?.type === EntityType.SchemaField &&
                    (entity as SchemaFieldEntity).parent.urn === resultEntityUrn,
            ),
        )
        .flat();
}

interface Props {
    paths: EntityPath[];
    resultEntityUrn: string;
    openModal: () => void;
}

export default function ColumnPathsText({ paths, resultEntityUrn, openModal }: Props) {
    const { lineageDirection } = useContext(LineageTabContext);

    const displayedColumns = getDisplayedColumns(paths, resultEntityUrn);

    if (!displayedColumns.length) return null;

    return (
        <>
            <DescriptionWrapper>
                {lineageDirection === LineageDirection.Downstream ? 'Downstream' : 'Upstream'} column
                {displayedColumns.length > 1 && 's'}:&nbsp;
            </DescriptionWrapper>
            <ResultText onClick={openModal}>
                <Tooltip
                    title={
                        <span>
                            Click to see column path{paths.length > 1 && 's'} from{' '}
                            <ColumnsRelationshipText displayedColumns={displayedColumns} />
                        </span>
                    }
                >
                    <span>
                        <DisplayedColumns displayedColumns={displayedColumns} />
                    </span>
                </Tooltip>
            </ResultText>
        </>
    );
}
