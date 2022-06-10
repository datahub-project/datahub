import React from 'react';
import styled from 'styled-components';
import { useGetSearchResultsForMultipleQuery } from '../../../graphql/search.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { EntityType, SearchResult } from '../../../types.generated';
import { ANTD_GRAY } from '../../entity/shared/constants';

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
    selectTag?: (urn: string, displayName: string, result: SearchResult) => void;
}

export default function TagBrowser({ selectTag }: Props) {
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

    const handleSelectTag = (urn: string, displayName: string, tag: SearchResult) => {
        if (selectTag) {
            selectTag(urn, displayName, tag);
        }
    };

    return (
        <div>
            {tagsResult?.map((tag) => {
                const displayName = entityRegistry.getDisplayName(tag.entity.type, tag.entity);
                return (
                    <TagWrapper>
                        <NameWrapper
                            showSelectStyles={!!selectTag}
                            onClick={() => {
                                handleSelectTag(tag.entity.urn, displayName, tag);
                            }}
                        >
                            {displayName}
                        </NameWrapper>
                    </TagWrapper>
                );
            })}
        </div>
    );
}
