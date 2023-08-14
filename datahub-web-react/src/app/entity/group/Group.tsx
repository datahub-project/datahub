import { TeamOutlined } from '@ant-design/icons';
import * as React from 'react';
import { CorpGroup, EntityType, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { Preview } from './preview/Preview';
import GroupProfile from './GroupProfile';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { HighlightProvider } from '../../search/highlight/HighlightContext';

/**
 * Definition of the DataHub CorpGroup entity.
 */
export class GroupEntity implements Entity<CorpGroup> {
    type: EntityType = EntityType.CorpGroup;

    // TODO: update icons for UserGroup
    icon = (fontSize: number, styleType: IconStyleType, color?: string) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <TeamOutlined style={{ fontSize, color }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <TeamOutlined style={{ fontSize, color }} />;
        }

        return (
            <TeamOutlined
                style={{
                    fontSize,
                    color: color || '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => false;

    isLineageEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    getPathName: () => string = () => 'group';

    getEntityName = () => 'Group';

    getCollectionName: () => string = () => 'Groups';

    renderProfile: (urn: string) => JSX.Element = (_) => <GroupProfile />;

    // todo - maybe we create a context called HighlightContext
    // we basically wrap any of these previews we care about highlighting with that
    // into that context we'll pass a matchedFields
    // each previewer components, tags/terms/etc will have to figure out how to highlight itself
    // but it can do a isHighlighted(field, value) from the context to figure out if we're highlighted
    // internally, the contexts keeps a shared map that can be used for the lookups
    renderPreview = (_: PreviewType, data: CorpGroup) => (
        <Preview
            urn={data.urn}
            name={this.displayName(data)}
            description={data.info?.description}
            membersCount={(data as any)?.memberCount?.total || 0}
        />
    );

    renderSearch = (result: SearchResult) => {
        const preview = this.renderPreview(PreviewType.SEARCH, result.entity as CorpGroup);
        console.log({ result });
        return <HighlightProvider matchedFields={result.matchedFields}>{preview}</HighlightProvider>;
    };

    displayName = (data: CorpGroup) => {
        return data.properties?.displayName || data.info?.displayName || data.name || data.urn;
    };

    getGenericEntityProperties = (group: CorpGroup) => {
        return getDataForEntityType({ data: group, entityType: this.type, getOverrideProperties: (data) => data });
    };

    supportedCapabilities = () => {
        return new Set([]);
    };
}
