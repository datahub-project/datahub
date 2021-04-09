import * as React from 'react';
import { DatabaseFilled, DatabaseOutlined } from '@ant-design/icons';
import { Tag, Typography } from 'antd';
import styled from 'styled-components';
import { Dataset, EntityType, RelatedDataset, SearchResult } from '../../../types.generated';
import { DatasetProfile } from './profile/DatasetProfile';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { Preview } from './preview/Preview';
import { FIELDS_TO_HIGHLIGHT } from './search/highlights';
import { Direction } from '../../lineage/types';

const MatchTag = styled(Tag)`
    &&& {
        margin-bottom: 0px;
        margin-top: 10px;
    }
`;

export default function getChildren(entity: Dataset, direction: Direction | null): Array<RelatedDataset> {
    if (direction === Direction.Upstream) {
        return entity.upstreamLineage?.upstreams || [];
    }

    if (direction === Direction.Downstream) {
        return entity.downstreamLineage?.downstreams || [];
    }

    return [];
}

/**
 * Definition of the DataHub Dataset entity.
 */
export class DatasetEntity implements Entity<Dataset> {
    type: EntityType = EntityType.Dataset;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DatabaseOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <DatabaseFilled style={{ fontSize, color: '#B37FEB' }} />;
        }

        return (
            <DatabaseFilled
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'dataset';

    getCollectionName = () => 'Datasets';

    renderProfile = (urn: string) => <DatasetProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: Dataset) => {
        return (
            <Preview
                urn={data.urn}
                name={data.name}
                origin={data.origin}
                description={data.description}
                platformName={data.platform.name}
                platformLogo={data.platform.info?.logoUrl}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as Dataset;
        return (
            <Preview
                urn={data.urn}
                name={data.name}
                origin={data.origin}
                description={data.description}
                platformName={data.platform.name}
                platformLogo={data.platform.info?.logoUrl}
                owners={data.ownership?.owners}
                globalTags={data.globalTags}
                snippet={
                    // Add match highlights only if all the matched fields are in the FIELDS_TO_HIGHLIGHT
                    result.matchedFields.length > 0 &&
                    result.matchedFields.every((field) => FIELDS_TO_HIGHLIGHT.has(field.name)) && (
                        <MatchTag>
                            <Typography.Text>
                                Matches {FIELDS_TO_HIGHLIGHT.get(result.matchedFields[0].name)}{' '}
                                <b>{result.matchedFields[0].value}</b>
                            </Typography.Text>
                        </MatchTag>
                    )
                }
            />
        );
    };

    getLineageVizConfig = (entity: Dataset) => {
        return {
            urn: entity.urn,
            name: entity.name,
            type: EntityType.Dataset,
            upstreamChildren: getChildren(entity, Direction.Upstream).map((child) => child.dataset.urn),
            downstreamChildren: getChildren(entity, Direction.Downstream).map((child) => child.dataset.urn),
            icon: entity.platform.info?.logoUrl || undefined,
        };
    };
}
