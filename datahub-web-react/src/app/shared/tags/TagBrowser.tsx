import React from 'react';
import styled from 'styled-components';
import { useGetSearchResultsForMultipleQuery } from '../../../graphql/search.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { EntityType, SearchResult, Tag } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';
import TagLabel from '../TagLabel';

const TagWrapper = styled.div`
    color: #262626;
    font-size: 12px;
    max-height: calc(100% - 47px);
    padding: 5px;
    overflow: auto;
`;

const nameStyles = `
    color: #262626;
    display: inline-block;
    height: 100%;
    padding: 3px 4px;
    width: 100%;
`;

export const NameWrapper = styled.span<{ showSelectStyles?: boolean }>`
    ${nameStyles}

    &:hover {
        ${(props) =>
            props.showSelectStyles &&
            `
        background-color: ${ANTD_GRAY[3]};
        cursor: pointer;
        `}
    }
`;

interface Props {
    selectBrowseTag?: (urn: string, displayName: string, result: SearchResult) => void;
}

export default function TagBrowser({ selectBrowseTag }: Props) {
    const entityRegistry = useEntityRegistry();
    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.Tag],
                query: '*',
                start: 0,
                count: 5,
            },
        },
    });
    const tagsResult = data?.searchAcrossEntities?.searchResults;

    const handleSelectBrowseTag = (urn: string, displayName: string, tag: SearchResult) => {
        if (selectBrowseTag) {
            selectBrowseTag(urn, displayName, tag);
        }
    };

    return (
        <div>
            {tagsResult?.map((tag) => {
                const displayName = entityRegistry.getDisplayName(tag.entity.type, tag.entity);
                return (
                    <TagWrapper>
                        <NameWrapper
                            showSelectStyles={!!selectBrowseTag}
                            onClick={() => {
                                handleSelectBrowseTag(tag.entity.urn, displayName, tag);
                            }}
                        >
                            <TagLabel
                                name={displayName}
                                colorHash={(tag.entity as Tag).urn}
                                color={(tag.entity as Tag).properties?.colorHex}
                            />
                        </NameWrapper>
                    </TagWrapper>
                );
            })}
        </div>
    );
}
