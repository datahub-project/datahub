import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

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
  personEntity: typeof PersonEntity = PersonEntity
): PersonEntity {
  entity.name = 'Ash Ketchum';
  entity.title = 'Pokemon master in training';
  entity.profilePictureUrl = 'https://i.imgur.com/vjLcuFJ.jpg';
  entity.email = 'ashketchumfrompallet@gmail.com';
  entity.linkedinProfile = 'https://www.linkedin.com/in/ash-ketchum-b212502a/';
  entity.slackLink = 'aketchum';
  entity.skills = ['training', 'catching', 'battling'];

  const managerEntity = new personEntity(personEntity.urnFromUsername('pikachu'));
  managerEntity.name = 'Pikachu';
  managerEntity.profilePictureUrl = 'https://pokemonletsgo.pokemon.com/assets/img/common/char-pikachu.png';

  entity.reportsTo = managerEntity;
  entity.teamTags = ['Kanta', 'Nintendo', 'Game Freak', 'Bike Thief'];
  entity.focusArea = 'Trying to catch all these Pokemon in the world. Also being the very best like no one ever was.';

  return entity;
}
