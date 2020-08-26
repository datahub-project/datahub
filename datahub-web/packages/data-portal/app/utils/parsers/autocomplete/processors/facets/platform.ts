import { IDataPlatform } from '@datahub/metadata-types/types/entity/dataset/platform';
import { ISuggestionBuilder } from 'datahub-web/utils/parsers/autocomplete/types';
import { readDataPlatforms } from '@datahub/data-models/api/dataset/platforms';

let platforms: Array<IDataPlatform>;

/**
 * when we expect to autosuggest a platform, we call backend to get the entire list
 * then cache it and do a local search.
 * TODO: META-7667 scope cache to function instead of module
 */
export const platform = async (builder: ISuggestionBuilder, facetValue: string): Promise<ISuggestionBuilder> => {
  if (!platforms) {
    platforms = await readDataPlatforms();
  }

  return {
    ...builder,
    facetNames: [
      ...builder.facetNames,
      ...platforms
        .filter(platform => platform.name.indexOf(facetValue) >= 0)
        .map(platform => ({
          title: `platform:${platform.name}`,
          text: `${builder.textPrevious}platform:${platform.name} `
        }))
    ]
  };
};
