import React, { useMemo, useState } from 'react';
import { Input, AutoComplete, Image, Typography } from 'antd';
import { SearchOutlined } from '@ant-design/icons';
import styled from 'styled-components';
import { useHistory } from 'react-router';
import { AutoCompleteResultForEntity, CorpUser, Entity, EntityType, ScenarioType, Tag } from '../../types.generated';
import { IconStyleType } from '../entity/Entity';
import EntityRegistry from '../entity/EntityRegistry';
import filterSearchQuery from './utils/filterSearchQuery';
import { ANTD_GRAY } from '../entity/shared/constants';
import { getEntityPath } from '../entity/shared/containers/profile/utils';
import { EXACT_SEARCH_PREFIX } from './utils/constants';
import { CustomAvatar } from '../shared/avatar';
import { StyledTag } from '../entity/shared/components/styled/StyledTag';
import { useListRecommendationsQuery } from '../../graphql/recommendations.generated';
import { useGetAuthenticatedUserUrn } from '../useGetAuthenticatedUser';

const SuggestionContainer = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: left;
    align-items: center;
`;

const SuggestionText = styled.span`
    margin-left: 8px;
    margin-top: 2px;
    margin-bottom: 2px;
    color: ${ANTD_GRAY[9]};
`;

const ExploreForEntity = styled.span`
    font-weight: light;
    font-size: 11px;
`;

const StyledAutoComplete = styled(AutoComplete)`
    width: 100%;
    max-width: 475px;
`;

const AutoCompleteContainer = styled.div`
    width: 100%;
    padding: 0 30px;
`;

const StyledSearchBar = styled(Input)`
    &&& {
        border-radius: 70px;
        height: 40px;
        font-size: 20px;
        color: ${ANTD_GRAY[7]};
    }
    > .ant-input {
        font-size: 14px;
    }
`;

const PreviewImage = styled(Image)`
    max-height: 16px;
    width: auto;
    object-fit: contain;
    background-color: transparent;
`;

const EXACT_AUTOCOMPLETE_OPTION_TYPE = 'exact_query';
const RECOMMENDED_QUERY_OPTION_TYPE = 'recommendation';

const renderTagSuggestion = (tag: Tag, registry: EntityRegistry) => {
    return (
        <>
            <StyledTag $colorHash={tag?.urn} $color={tag?.properties?.colorHex}>
                {registry.getDisplayName(EntityType.Tag, tag)}
            </StyledTag>
        </>
    );
};

const renderUserSuggestion = (query: string, user: CorpUser, registry: EntityRegistry) => {
    const displayName = registry.getDisplayName(EntityType.CorpUser, user);
    const isPrefixMatch = displayName.toLowerCase().indexOf(query.toLowerCase()) > -1;
    const matchedText = (isPrefixMatch && displayName.substring(0, query.length)) || '';
    const unmatchedText = (isPrefixMatch && displayName.substring(query.length, displayName.length)) || displayName;
    return (
        <>
            <CustomAvatar
                size={20}
                name={displayName}
                photoUrl={user.editableProperties?.pictureLink || undefined}
                useDefaultAvatar={false}
                style={{
                    marginRight: 0,
                }}
            />
            <SuggestionText>
                {matchedText}
                <Typography.Text strong>{unmatchedText}</Typography.Text>
            </SuggestionText>
        </>
    );
};

const renderEntitySuggestion = (query: string, entity: Entity, registry: EntityRegistry) => {
    // Special rendering.
    if (entity.type === EntityType.CorpUser) {
        return renderUserSuggestion(query, entity as CorpUser, registry);
    }
    if (entity.type === EntityType.Tag) {
        return renderTagSuggestion(entity as Tag, registry);
    }
    const genericEntityProps = registry.getGenericEntityProperties(entity.type, entity);
    const platformName = genericEntityProps?.platform?.properties?.displayName || genericEntityProps?.platform?.name;
    const platformLogoUrl = genericEntityProps?.platform?.properties?.logoUrl;
    const displayName =
        genericEntityProps?.properties?.qualifiedName ||
        genericEntityProps?.name ||
        registry.getDisplayName(entity.type, entity);
    const icon =
        (platformLogoUrl && <PreviewImage preview={false} src={platformLogoUrl} alt={platformName || ''} />) ||
        registry.getIcon(entity.type, 12, IconStyleType.ACCENT);
    const isPrefixMatch = displayName.toLowerCase().startsWith(query.toLowerCase());
    const matchedText = isPrefixMatch ? displayName.substring(0, query.length) : '';
    const unmatchedText = isPrefixMatch ? displayName.substring(query.length, displayName.length) : displayName;
    return (
        <>
            {icon}
            <SuggestionText>
                <Typography.Text strong>{matchedText}</Typography.Text>
                {unmatchedText}
            </SuggestionText>
        </>
    );
};

const renderItem = (query: string, entity: Entity, registry: EntityRegistry) => {
    return {
        value: entity.urn,
        label: <SuggestionContainer>{renderEntitySuggestion(query, entity, registry)}</SuggestionContainer>,
        type: entity.type,
        style: { paddingLeft: 16 },
    };
};

const renderRecommendedQuery = (query: string) => {
    return {
        value: query,
        label: query,
        type: RECOMMENDED_QUERY_OPTION_TYPE,
    };
};

interface Props {
    initialQuery?: string;
    placeholderText: string;
    suggestions: Array<AutoCompleteResultForEntity>;
    onSearch: (query: string, type?: EntityType) => void;
    onQueryChange: (query: string) => void;
    style?: React.CSSProperties;
    inputStyle?: React.CSSProperties;
    autoCompleteStyle?: React.CSSProperties;
    entityRegistry: EntityRegistry;
    fixAutoComplete?: boolean;
}

const defaultProps = {
    style: undefined,
};

/**
 * Represents the search bar appearing in the default header view.
 */
export const SearchBar = ({
    initialQuery,
    placeholderText,
    suggestions,
    onSearch,
    onQueryChange,
    entityRegistry,
    style,
    inputStyle,
    autoCompleteStyle,
    fixAutoComplete,
}: Props) => {
    const history = useHistory();
    const [searchQuery, setSearchQuery] = useState<string>();
    const [selected, setSelected] = useState<string>();
    const searchEntityTypes = entityRegistry.getSearchEntityTypes();
    const userUrn = useGetAuthenticatedUserUrn();
    const { data } = useListRecommendationsQuery({
        variables: {
            input: {
                userUrn,
                requestContext: {
                    scenario: ScenarioType.SearchBar,
                },
                limit: 1,
            },
        },
    });

    const effectiveQuery = searchQuery !== undefined ? searchQuery : initialQuery || '';

    const emptyQueryOptions = useMemo(() => {
        // Map each module to a set of
        return (
            data?.listRecommendations?.modules.map((module) => ({
                label: module.title,
                options: [...module.content.map((content) => renderRecommendedQuery(content.value))],
            })) || []
        );
    }, [data]);

    const autoCompleteQueryOptions = useMemo(
        () =>
            (suggestions?.length > 0 &&
                effectiveQuery.length > 0 && [
                    {
                        value: `${EXACT_SEARCH_PREFIX}${effectiveQuery}`,
                        label: (
                            <SuggestionContainer key={EXACT_AUTOCOMPLETE_OPTION_TYPE}>
                                <ExploreForEntity>
                                    View all results for <Typography.Text strong>{effectiveQuery}</Typography.Text>
                                </ExploreForEntity>
                            </SuggestionContainer>
                        ),
                        type: EXACT_AUTOCOMPLETE_OPTION_TYPE,
                    },
                ]) ||
            [],
        [suggestions, effectiveQuery],
    );

    const autoCompleteEntityOptions = useMemo(
        () =>
            suggestions.map((entity: AutoCompleteResultForEntity) => ({
                label: entityRegistry.getCollectionName(entity.type),
                options: [...entity.entities.map((e: Entity) => renderItem(effectiveQuery, e, entityRegistry))],
            })),
        [effectiveQuery, suggestions, entityRegistry],
    );

    const options = useMemo(() => {
        // Display recommendations when there is no search query, autocomplete suggestions otherwise.
        if (autoCompleteEntityOptions.length > 0) {
            return [...autoCompleteQueryOptions, ...autoCompleteEntityOptions];
        }
        return emptyQueryOptions;
    }, [emptyQueryOptions, autoCompleteEntityOptions, autoCompleteQueryOptions]);

    return (
        <AutoCompleteContainer style={style}>
            <StyledAutoComplete
                defaultActiveFirstOption={false}
                style={autoCompleteStyle}
                options={options}
                filterOption={false}
                onSelect={(value: string, option) => {
                    // If the autocomplete option type is NOT an entity, then render as a normal search query.
                    if (
                        option.type === EXACT_AUTOCOMPLETE_OPTION_TYPE ||
                        option.type === RECOMMENDED_QUERY_OPTION_TYPE
                    ) {
                        onSearch(
                            `${filterSearchQuery(value)}`,
                            searchEntityTypes.indexOf(option.type) >= 0 ? option.type : undefined,
                        );
                    } else {
                        // Navigate directly to the entity profile.
                        history.push(getEntityPath(option.type, value, entityRegistry, false));
                    }
                }}
                onSearch={(value: string) => onQueryChange(value)}
                defaultValue={initialQuery || undefined}
                value={selected}
                onChange={(v) => setSelected(filterSearchQuery(v))}
                dropdownStyle={{
                    maxHeight: 1000,
                    overflowY: 'visible',
                    position: (fixAutoComplete && 'fixed') || 'relative',
                }}
            >
                <StyledSearchBar
                    placeholder={placeholderText}
                    onPressEnter={() => {
                        // e.stopPropagation();
                        onSearch(filterSearchQuery(searchQuery || ''));
                    }}
                    style={inputStyle}
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    data-testid="search-input"
                    prefix={<SearchOutlined onClick={() => onSearch(filterSearchQuery(searchQuery || ''))} />}
                />
            </StyledAutoComplete>
        </AutoCompleteContainer>
    );
};

SearchBar.defaultProps = defaultProps;
