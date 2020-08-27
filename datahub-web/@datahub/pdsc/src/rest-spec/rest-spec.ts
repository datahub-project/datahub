// tslint:disable:max-classes-per-file

export class Finder {
  name: string;
  doc: string;
  parameters: Array<{
    name: string;
    type: string;
    doc: string;
  }>;
}

/**
 * Define the format of the rest-spec.json files.
 *
 * This is an incomplete definition, just the bits that we need to translate
 * for now.
 */
export interface RestSpec {
  name: string;
  path: string;
  namespace: string;
  schema: string;
  doc: string;
  collection: {
    identifier: string;
    type: string;
    finders: Finder[];
  };
}
