import { INodeFacetProcessor, ISuggestionBuilder } from 'datahub-web/utils/parsers/autocomplete/types';
import { RecordValue } from 'datahub-web/typings/generic';

export const localFacetProcessor = (facetName: string, values: Array<string>): RecordValue<INodeFacetProcessor> => {
  /**
   * when we expect to auto suggest a fabric, we just do a local search for the fabrics available
   */

  return (builder: ISuggestionBuilder, facetValue: string): Promise<ISuggestionBuilder> =>
    Promise.resolve({
      ...builder,
      facetNames: [
        ...builder.facetNames,
        ...values
          .filter(value => value.includes(facetValue))
          .map(value => ({
            title: `${facetName}:${value}`,
            text: `${builder.textPrevious}${facetName}:${value} `
          }))
      ]
    });
};
