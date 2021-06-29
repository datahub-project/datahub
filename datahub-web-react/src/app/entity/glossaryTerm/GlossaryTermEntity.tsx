import * as React from 'react';
import { BookFilled, BookOutlined } from '@ant-design/icons';
import { EntityType, GlossaryTerm, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { Preview } from './preview/Preview';
import GlossaryTermProfile from './profile/GlossaryTermProfile';

/**
 * Definition of the DataHub Dataset entity.
 */
export class GlossaryTermEntity implements Entity<GlossaryTerm> {
    type: EntityType = EntityType.GlossaryTerm;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <BookOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <BookFilled style={{ fontSize, color: '#B37FEB' }} />;
        }

        return (
            <BookOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => false;

    isBrowseEnabled = () => false;

    getAutoCompleteFieldName = () => 'name';

    isLineageEnabled = () => false;

    getPathName = () => 'glossary';

    getCollectionName = () => 'Business Glossary';

    renderProfile: (urn: string) => JSX.Element = (_) => <GlossaryTermProfile />;

    renderSearch = (result: SearchResult) => {
        return this.renderPreview(PreviewType.SEARCH, result.entity as GlossaryTerm);
    };

    renderPreview = (_: PreviewType, data: GlossaryTerm) => {
        return (
            <Preview
                urn={data?.urn}
                name={data?.name}
                definition={data?.glossaryTermInfo?.definition}
                owners={data?.ownership?.owners}
            />
        );
    };
}
