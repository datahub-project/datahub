import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';

/**
 * Interface representing an object that contains the properties and typings associated with them
 * for items in the Person Entity that we deem to be editable
 */
export interface IPersonEntityEditableProperties {
  // User-provided focus area, describing themselves and what they do
  focusArea: PersonEntity['focusArea'];
  // A list of skills that this particular person entity has declared to own.
  skills: PersonEntity['skills'];
  // Tags that in aggregate denote which team and organization to which the user belongs
  teamTags: PersonEntity['teamTags'];
}
