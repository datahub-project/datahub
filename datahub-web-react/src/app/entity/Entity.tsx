import { EntityType } from '../../types.generated';

export enum PreviewType {
    /**
     * A preview shown within the search experience
     */
    SEARCH,
    /**
     * A preview shown within the browse experience
     */
    BROWSE,
    /**
     * A generic preview shown within other entity pages, etc.
     */
    PREVIEW,
}

/**
 * Base interface used for authoring DataHub Entities on the client side.
 *
 * <T> the generated GraphQL data type associated with the entity.
 */
export interface Entity<T> {
    /**
     * Corresponding GQL EntityType.
     */
    type: EntityType;

    /**
     * Returns whether the entity search is enabled
     */
    isSearchEnabled: () => boolean;

    /**
     * Returns whether the entity browse is enabled
     */
    isBrowseEnabled: () => boolean;

    /**
     * Returns the name of the entity as it appears in a URL, e.g. '/dataset/:urn'.
     */
    getPathName: () => string;

    /**
     * Returns the plural name of the entity used when displaying collections (search, browse results), e.g. 'Datasets'.
     */
    getCollectionName: () => string;

    /**
     * Renders the 'profile' of the entity on an entity details page.
     */
    renderProfile: (urn: string) => JSX.Element;

    /**
     * Renders a preview of the entity across different use cases like search, browse, etc.
     */
    renderPreview: (type: PreviewType, data: T) => JSX.Element;
}
