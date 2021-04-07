import * as React from 'react';
import { DatabaseFilled, DatabaseOutlined } from '@ant-design/icons';
import { Tag, Typography } from 'antd';
import styled from 'styled-components';
import { DataFlow, EntityType, SearchResult } from '../../../types.generated';
import { Preview } from './preview/Preview';
import { DataFlowProfile } from './profile/DataFlowProfile';
import { Entity, IconStyleType, PreviewType } from '../Entity';

const MatchTag = styled(Tag)`
    &&& {
        margin-bottom: 0px;
        margin-top: 10px;
    }
`;

const FIELDS_TO_HIGHLIGHT = new Map();
// FIELDS_TO_HIGHLIGHT.set('fieldPaths', 'column');

/**
 * Definition of the DataHub DataFlow entity.
 */
export class DataFlowEntity implements Entity<DataFlow> {
    type: EntityType = EntityType.DataFlow;

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

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'dataFlow';

    getCollectionName = () => 'DataFlows';

    renderProfile = (urn: string) => <DataFlowProfile urn={urn} />;

    renderPreview = (_: PreviewType, data: DataFlow) => {
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.info?.description || ''}
                platformName={data.info?.project || ''}
                owners={data.ownership?.owners}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as DataFlow;
        return (
            <Preview
                urn={data.urn}
                name={data.info?.name || ''}
                description={data.info?.description || ''}
                platformName={data.info?.project || ''}
                owners={data.ownership?.owners}
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
}
