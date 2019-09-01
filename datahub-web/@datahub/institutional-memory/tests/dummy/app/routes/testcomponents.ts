import Route from '@ember/routing/route';
import { IInstitutionalMemory } from '@datahub/data-models/types/entity/common/wiki/institutional-memory';
import { InstitutionalMemory } from '@datahub/data-models/models/aspects/institutional-memory';

export default class Testcomponents extends Route {
  model(): { linkList: Array<IInstitutionalMemory> } {
    const testInstitutionalMemory: Array<IInstitutionalMemory> = [
      {
        url: 'https://www.serebii.net/pokedex-sm/025.shtml',
        description: 'Pikachu page',
        createStamp: { actor: 'aketchum', time: 1556561920 }
      },
      {
        url: 'https://www.serebii.net/pokedex-sm/133.shtml',
        description: 'Eevee page',
        createStamp: { actor: 'goak', time: 1556571920 }
      }
    ];

    return {
      linkList: testInstitutionalMemory.map(link => new InstitutionalMemory(link))
    };
  }
}
