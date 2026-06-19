import { Tooltip } from '@components';
import React, { useContext } from 'react';
import { Trans, useTranslation } from 'react-i18next';
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
    margin-right: 4px;
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
    const { t } = useTranslation('entity.preview');
    const { lineageDirection } = useContext(LineageTabContext);

    const displayedColumns = getDisplayedColumns(paths, resultEntityUrn);

    if (!displayedColumns.length) return null;

    return (
        <>
            <DescriptionWrapper>
                {t(
                    lineageDirection === LineageDirection.Downstream
                        ? 'columnPaths.downstreamColumnsLabel'
                        : 'columnPaths.upstreamColumnsLabel',
                    { count: displayedColumns.length },
                )}
            </DescriptionWrapper>
            <ResultText data-testid="column-path-modal-trigger" onClick={openModal}>
                <Tooltip
                    title={
                        <Trans
                            t={t}
                            i18nKey="columnPaths.tooltip"
                            count={paths.length}
                            components={{
                                relationship: <ColumnsRelationshipText displayedColumns={displayedColumns} />,
                            }}
                        />
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
