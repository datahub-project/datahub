/**
 * Owner category or owner role
 * @export
 * @enum {string}
 */
export enum OwnershipType {
  // A person or group that is in charge of developing the code
  Developer = 'DEVELOPER',
  // A person who has the most knowledge. Oftentimes also the lead developer.
  SubjectMatterExpert = 'SUBJECT_MATTER_EXPERT',
  // A person or a group that overseas the operation, e.g. a DBA or SRE.
  Delegate = 'DELEGATE',
  // A person, group, or service that produces/generates the data
  Producer = 'PRODUCER',
  // A person, group, or service that consumes the data
  Consumer = 'CONSUMER',
  // A person or a group that has direct business interest
  Stakeholder = 'STAKEHOLDER'
}
