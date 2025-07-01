/**
 * Configs used to render the recipe builder experience for a particular source.
 */
export interface SourceConfig {
    /**
     * The 'type' of the source, which should align with the recipe type.
     */
    type: string;

    /**
     * A placeholder recipe to show for the source.
     */
    placeholderRecipe: string;

    /**
     * TODO: A json schema for the 'source' block of the recipe to use for validation.
     * Could also be a validate function.
     */
    // sourceSchema: string;

    /**
     * The display name for the source.
     */
    displayName: string;

    /**
     * The url to the docs about the source.
     */
    docsUrl?: string;

    /**
     * The url to a logo for the source.
     */
    logoUrl?: string;

    /**
     * A react component to use in place of a logo.
     */
    logoComponent?: React.ReactNode;
}
