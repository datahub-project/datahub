import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { IPersonEntityEditableProperties } from '@datahub/data-models/types/entity/person/props';
import { setProperties } from '@ember/object';

// TODO: [META-9851] Temporary home for this function for integrated dev/demo, should be moved to test/dummy folder eventually
/**
 * Allows us for demo purposes or testing purposes to provide mock data directly on a class
 * instance for a person entity.
 * @param {PersonEntity} entity - the entity instance we want to decorate
 * @param {typeof PersonEntity} personEntity - the class object itself. Optional. Passed in if we
 *  want to utilize the implementation of the class not defined in the open sourced version
 */
export function populateMockPersonEntity(
  entity: PersonEntity,
  PersonEntityClass: typeof PersonEntity = PersonEntity
): PersonEntity {
  const managerEntity = Object.assign(
    {},
    new PersonEntity(PersonEntityClass.urnFromUsername ? PersonEntityClass.urnFromUsername('pikachu') : 'pikachu'),
    {
      name: 'Pikachu',
      profilePictureUrl: 'https://pokemonletsgo.pokemon.com/assets/img/common/char-pikachu.png'
    }
  );

  const newEntity = Object.assign({}, entity, {
    name: 'Ash Ketchum',
    title: 'Pokemon master in training',
    profilePictureUrl: 'https://i.imgur.com/vjLcuFJ.jpg',
    email: 'ashketchumfrompallet@gmail.com',
    linkedinProfile: 'https://www.linkedin.com/in/ash-ketchum-b212502a/',
    slackLink: 'aketchum',
    skills: ['training', 'catching', 'battling'],
    teamTags: ['Kanto', 'Nintendo', 'Game Freak', 'Bike Thief'],
    focusArea: 'Trying to catch all these Pokemon in the world. Also being the very best like no one ever was.',
    reportsTo: managerEntity,
    updateEditableProperties: async function(value: IPersonEntityEditableProperties): Promise<void> {
      await setProperties(newEntity, value);
    }
  });

  return newEntity;
}
