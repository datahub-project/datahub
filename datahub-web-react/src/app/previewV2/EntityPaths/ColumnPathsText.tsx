import { Tooltip } from '@components';
import React, { useContext } from 'react';
import styled from 'styled-components/macro';

import { LineageTabContext } from '@app/entityV2/shared/tabs/Lineage/LineageTabContext';
import ColumnsRelationshipText from '@app/previewV2/EntityPaths/ColumnsRelationshipText';
import DisplayedColumns from '@app/previewV2/EntityPaths/DisplayedColumns';

import { EntityPath, EntityType, LineageDirection, SchemaFieldEntity } from '@types';

const ResultText = styled.span`
    white-space: nowrap;
    &:hover {
        border-bottom: 1px solid ${(props) => props.theme.colors.border};
        cursor: pointer;
    }
`;

const DescriptionWrapper = styled.span`
    color: ${(props) => props.theme.colors.textSecondary};
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
            {/* eslint-disable i18next/no-literal-string -- (untranslated-text) lineage column-path phrase; deferred to follow-up */}
            <DescriptionWrapper>
                {lineageDirection === LineageDirection.Downstream ? 'Downstream' : 'Upstream'} column
                {displayedColumns.length > 1 && 's'}:&nbsp;
            </DescriptionWrapper>
            {/* eslint-enable i18next/no-literal-string */}
            <ResultText onClick={openModal}>
                <Tooltip
                    title={
                        /* eslint-disable i18next/no-literal-string -- (untranslated-text) lineage column-path phrase; deferred to follow-up */
                        <span>
                            Click to see column path{paths.length > 1 && 's'} from{' '}
                            <ColumnsRelationshipText displayedColumns={displayedColumns} />
                        </span>
                        /* eslint-enable i18next/no-literal-string */
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
