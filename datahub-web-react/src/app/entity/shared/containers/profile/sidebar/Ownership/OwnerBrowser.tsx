import React from 'react';
import styled from 'styled-components';
import { useGetSearchResultsForMultipleQuery } from '../../../../../../../graphql/search.generated';
import { SearchResult, EntityType, CorpUser } from '../../../../../../../types.generated';
import { useEntityRegistry } from '../../../../../../useEntityRegistry';
import { OwnerLabel } from '../../../../../../shared/OwnerLabel';
import { ANTD_GRAY } from '../../../../constants';

const OwnersBrowseWrapper = styled.div`
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

export const OwnersContentWrapper = styled.span<{ showSelectStyles?: boolean }>`
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
    selectBrowseOwner?: (urn: string, result: SearchResult) => void;
}

export default function OwnerBrowser({ selectBrowseOwner }: Props) {
    const entityRegistry = useEntityRegistry();
    const { data } = useGetSearchResultsForMultipleQuery({
        variables: {
            input: {
                types: [EntityType.CorpGroup, EntityType.CorpUser],
                query: '*',
                start: 0,
                count: 5,
            },
        },
    });
    const ownersResult = data?.searchAcrossEntities?.searchResults;

    const handleSelectBrowseOwner = (urn: string, ownerResult: SearchResult) => {
        if (selectBrowseOwner) {
            selectBrowseOwner(urn, ownerResult);
        }
    };

    return (
        <div>
            {ownersResult?.map((ownerResult) => {
                const displayName = entityRegistry.getDisplayName(ownerResult.entity.type, ownerResult.entity);
                const avatarUrl =
                    ownerResult.entity.type === EntityType.CorpUser
                        ? (ownerResult.entity as CorpUser).editableProperties?.pictureLink || undefined
                        : undefined;
                return (
                    <OwnersBrowseWrapper>
                        <OwnersContentWrapper
                            showSelectStyles={!!selectBrowseOwner}
                            onClick={() => {
                                handleSelectBrowseOwner(ownerResult.entity.urn, ownerResult);
                            }}
                        >
                            <OwnerLabel name={displayName} avatarUrl={avatarUrl} type={ownerResult.entity.type} />
                        </OwnersContentWrapper>
                    </OwnersBrowseWrapper>
                );
            })}
        </div>
    );
}
