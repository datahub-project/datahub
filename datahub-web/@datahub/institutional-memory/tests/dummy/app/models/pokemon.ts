import { BaseEntity } from '@datahub/data-models/entity/base-entity';
import { IBaseEntity } from '@datahub/metadata-types/types/entity';
import { InstitutionalMemories, InstitutionalMemory } from '@datahub/data-models/models/aspects/institutional-memory';
import { getJSON, postJSON } from '@datahub/utils/api/fetcher';
import { set } from '@ember/object';
import { IInstitutionalMemory } from '@datahub/metadata-types/types/aspects/institutional-memory';

export interface IPokemon extends IBaseEntity {
  urn: string;
  removed: boolean;
  name: string;
  type: string;
}

const getPokemonIMUrlByUrn = (urn: string): string => `/pokemons/${urn.replace(/:/g, 'h')}/institutionalmemory`;

export class Pokemon extends BaseEntity<IPokemon> {
  urn: string;

  get displayName(): string {
    return 'pokemons';
  }

  async readInstitutionalMemory(): Promise<InstitutionalMemories> {
    const { elements: institutionalMemories } = await getJSON<{ elements: Array<IInstitutionalMemory> }>({
      url: getPokemonIMUrlByUrn(this.urn)
    });
    const institutionalMemoriesMap = institutionalMemories.map(memory => new InstitutionalMemory(memory));
    set(this, 'institutionalMemories', institutionalMemoriesMap);
    return institutionalMemoriesMap;
  }

  async writeInstitutionalMemory(): Promise<void> {
    if (this.institutionalMemories) {
      await postJSON({
        url: getPokemonIMUrlByUrn(this.urn),
        data: {
          elements: this.institutionalMemories.map(memory => memory.readWorkingCopy())
        }
      });
    }
  }

  constructor(urn: string) {
    super(urn);
    this.urn = urn;
  }
}
