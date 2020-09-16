import { IInstitutionalMemory } from '@datahub/metadata-types/types/aspects/institutional-memory';

type SchemaDb<T> = Array<T> & {
  where: (query: Partial<T>) => Array<T>;
  update: (query: Partial<T>) => void;
  remove: (query?: Partial<T>) => void;
  insert: (item: T) => void;
};

// TODO: [META-9051] We are exposing metadata types here, and need to figure out where these mirage
// definitions should live in order to not expose the API layer to the open source world
export interface IMirageInstitutionalMemorySchema {
  db: {
    institutionalMemories: SchemaDb<IInstitutionalMemory>;
  };
}
