import React from 'react';
import { Button, Checkbox, Divider, Empty, List, ListProps } from 'antd';
import styled from 'styled-components';
import { useHistory } from 'react-router';
import { RocketOutlined } from '@ant-design/icons';
import { navigateToSearchUrl } from './utils/navigateToSearchUrl';
import { ANTD_GRAY } from '../entity/shared/constants';
import { CombinedSearchResult, SEPARATE_SIBLINGS_URL_PARAM } from '../entity/shared/siblingUtils';
import { CompactEntityNameList } from '../recommendations/renderer/component/CompactEntityNameList';
import { useEntityRegistry } from '../useEntityRegistry';
import { SearchResult } from '../../types.generated';
import analytics, { EventType } from '../analytics';
import { EntityAndType } from '../entity/shared/types';

const ResultList = styled(List)`
    &&& {
        width: 100%;
        border-color: ${(props) => props.theme.styles['border-color-base']};
        margin-top: 8px;
        padding: 16px 32px;
        border-radius: 0px;
    }
`;

const StyledCheckbox = styled(Checkbox)`
    margin-right: 12px;
`;

const NoDataContainer = styled.div`
    > div {
        margin-top: 28px;
        margin-bottom: 28px;
    }
`;

const ThinDivider = styled(Divider)`
    margin-top: 16px;
    margin-bottom: 16px;
`;

const SiblingResultContainer = styled.div`
    margin-top: 6px;
`;

const ListItem = styled.div<{ isSelectMode: boolean }>`
    display: flex;
    align-items: center;
    padding: 0px;
`;

type Props = {
    query: string;
    searchResults: CombinedSearchResult[];
    totalResultCount: number;
    isSelectMode: boolean;
    selectedEntities: EntityAndType[];
    setSelectedEntities: (entities: EntityAndType[]) => any;
};

export const SearchResultList = ({
    query,
    searchResults,
    totalResultCount,
    isSelectMode,
    selectedEntities,
    setSelectedEntities,
}: Props) => {
    const history = useHistory();
    const entityRegistry = useEntityRegistry();
    const selectedEntityUrns = selectedEntities.map((entity) => entity.urn);

    const onClickResult = (result: SearchResult, index: number) => {
        analytics.event({
            type: EventType.SearchResultClickEvent,
            query,
            entityUrn: result.entity.urn,
            entityType: result.entity.type,
            index,
            total: totalResultCount,
        });
    };

    /**
     * Invoked when a new entity is selected. Simply updates the state of the list of selected entities.
     */
    const onSelectEntity = (selectedEntity: EntityAndType, selected: boolean) => {
        if (selected) {
            setSelectedEntities?.([...selectedEntities, selectedEntity]);
        } else {
            setSelectedEntities?.(selectedEntities?.filter((entity) => entity.urn !== selectedEntity.urn) || []);
        }
    };

    return (
        <>
            <ResultList<React.FC<ListProps<CombinedSearchResult>>>
                dataSource={searchResults}
                split={false}
                locale={{
                    emptyText: (
                        <NoDataContainer>
                            <Empty
                                style={{ fontSize: 18, color: ANTD_GRAY[8] }}
                                description={`No results found for "${query}"`}
                            />
                            <Button onClick={() => navigateToSearchUrl({ query: '*', page: 0, history })}>
                                <RocketOutlined /> Explore all
                            </Button>
                        </NoDataContainer>
                    ),
                }}
                renderItem={(item, index) => (
                    <>
                        <ListItem
                            isSelectMode={isSelectMode}
                            onClick={() => onClickResult(item, index)}
                            // class name for counting in test purposes only
                            className="test-search-result"
                        >
                            {isSelectMode && (
                                <StyledCheckbox
                                    checked={selectedEntityUrns.indexOf(item.entity.urn) >= 0}
                                    onChange={(e) =>
                                        onSelectEntity(
                                            { urn: item.entity.urn, type: item.entity.type },
                                            e.target.checked,
                                        )
                                    }
                                />
                            )}
                            {entityRegistry.renderSearchResult(item.entity.type, item)}
                        </ListItem>
                        {item.matchedEntities && item.matchedEntities.length > 0 && (
                            <SiblingResultContainer className="test-search-result-sibling-section">
                                <CompactEntityNameList
                                    linkUrlParams={{ [SEPARATE_SIBLINGS_URL_PARAM]: true }}
                                    entities={item.matchedEntities}
                                />
                            </SiblingResultContainer>
                        )}
                        <ThinDivider />
                    </>
                )}
            />
        </>
    );
};
