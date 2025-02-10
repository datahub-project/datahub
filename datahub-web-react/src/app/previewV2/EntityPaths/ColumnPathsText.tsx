import { Tooltip } from '@components';
import React, { useContext } from 'react';
import styled from 'styled-components/macro';
import { EntityPath, EntityType, LineageDirection, SchemaFieldEntity } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import ColumnsRelationshipText from './ColumnsRelationshipText';
import DisplayedColumns from './DisplayedColumns';
import { LineageTabContext } from '../../entityV2/shared/tabs/Lineage/LineageTabContext';

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
