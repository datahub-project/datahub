import { PersonEntity } from '@datahub/data-models/entity/person/person-entity';
import { oneWay } from '@ember/object/computed';

/**
 * Creates more specific items to test the generic PersonEntity instance when needed.
 */
export class TestPersonEntity extends PersonEntity {
  /**
   * Allows us to manually set directReportUrns from outside the class by modifying this property
   * instead
   */
  manualDirectReportUrns: Array<string> = [];

  /**
   * Because there's no specific implementation for directReportUrns, which is a computed property,
   * we cannot manually set this property from outside the class in the open source PersonEntity
   * class. This is an issue as we want to test the @relationship decorator for directReports,
   * which relies on this property. Therefore, we override the definition here with a "proper"
   * (sort of) implementation in order to run those tests.
   */
  @oneWay('manualDirectReportUrns')
  directReportUrns!: Array<string>;

  /**
   * Same as manualDirectReportUrns, for testing peersUrns relationship
   */
  manualPeersUrns: Array<string> = [];

  /**
   * Same as directReportsUrns, to test peers relationship
   */
  @oneWay('manualPeersUrns')
  peersUrns!: Array<string>;
}
