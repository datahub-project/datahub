import { DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import { IInstitutionalMemory } from '@datahub/metadata-types/types/aspects/institutional-memory';
import {
  readInstitutionalMemory as readInstitutionalMemoryApi,
  writeInstitutionalMemory as writeInstitutionalMemoryApi
} from '@datahub/data-models/api/common/institutional-memory';
import { returnDefaultIfNotFound } from '@datahub/utils/api/fetcher';
import { InstitutionalMemory } from '@datahub/data-models/models/aspects/institutional-memory';
import { NotImplementedError } from '@datahub/data-models/constants/entity/shared';
import { set } from '@ember/object';

// TODO META-12149 this should be part of an Aspect. This fns can't live under BaseEntity as
// then we would have a circular dependency:
// BaseEntity -> InstitutionalMemory -> PersonEntity -> BaseEntity
/**
 * Retrieves a list of wiki documents related to the particular entity instance
 * @readonly
 */
export async function readInstitutionalMemory(this: DataModelEntityInstance): Promise<Array<InstitutionalMemory>> {
  const apiEntityName = this.staticInstance.renderProps.apiEntityName;
  if (apiEntityName) {
    // Handling for expected possibility of receiving a 404 for institutional memory for this dataset, which would
    // likely mean nothing has been added yet and we should allow the user to be the first to add something
    const { elements: institutionalMemories } = await returnDefaultIfNotFound(
      readInstitutionalMemoryApi(this.urn, apiEntityName),
      {
        elements: [] as Array<IInstitutionalMemory>
      }
    );

    const institutionalMemoriesMap = institutionalMemories.map(
      (link): InstitutionalMemory => new InstitutionalMemory(link)
    );
    set(this, 'institutionalMemories', institutionalMemoriesMap);
    return institutionalMemoriesMap;
  } else {
    throw new Error(NotImplementedError);
  }
}

/**
 * Writes a list of wiki documents related to a particular entity instance to the api layer
 */
export async function writeInstitutionalMemory(this: DataModelEntityInstance): Promise<void> {
  const apiEntityName = this.staticInstance.renderProps.apiEntityName;
  if (apiEntityName) {
    const { institutionalMemories } = this;
    institutionalMemories &&
      (await writeInstitutionalMemoryApi(
        this.urn,
        apiEntityName,
        institutionalMemories.map((link): IInstitutionalMemory => link.readWorkingCopy())
      ));
  } else {
    throw new Error(NotImplementedError);
  }
}
