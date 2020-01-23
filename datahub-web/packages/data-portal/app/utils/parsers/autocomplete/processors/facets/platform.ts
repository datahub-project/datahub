import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';
import { ISuggestionBuilder } from 'wherehows-web/utils/parsers/autocomplete/types';
import { readDataPlatforms } from '@datahub/data-models/api/dataset/platforms';

/**
 * when we expect to autosuggest a platform, we call backend to get the entire list
 * then cache it and do a local search.
 */
export const getPlatformProcessor = (
  defaultPlatforms: Array<IDataPlatform> = []
): ((builder: ISuggestionBuilder, facetValue: string) => Promise<ISuggestionBuilder>) => {
  let platforms: Array<IDataPlatform> = defaultPlatforms;

  return async (builder: ISuggestionBuilder, facetValue: string): Promise<ISuggestionBuilder> => {
    if (!platforms) {
      platforms = await readDataPlatforms();
    }

    return {
      ...builder,
      facetNames: [
        ...builder.facetNames,
        ...platforms
          .filter((platform: IDataPlatform): boolean => platform.name.indexOf(facetValue) >= 0)
          .map((platform): { title: string; text: string } => ({
            title: `platform:${platform.name}`,
            text: `${builder.textPrevious}platform:${platform.name} `
          }))
      ]
    };
  };
};
