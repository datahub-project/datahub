import { faker } from 'ember-cli-mirage';
import { dasherize } from '@ember/string';

export const getAutocompleteDatasets = function(_: any, request: any) {
  return {
    status: 'ok',
    input: request.queryParams.input,

    // in order to check that we cached things, we generate the list of words randomly
    // if change, please check search-box-test.ts
    source: [...Array(10)].map(() => dasherize(faker.lorem.words().toLowerCase()))
  };
};
