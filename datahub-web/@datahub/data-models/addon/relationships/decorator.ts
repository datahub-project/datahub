import { computed } from '@ember/object';
import { DataModelName, DataModelEntityInstance } from '@datahub/data-models/constants/entity';
import {
  IRelationshipDecoratedClassPrototype,
  DataModelsRelationshipGetter
} from '@datahub/data-models/types/relationships/decorator-types';

/**
 * Decorates a property in a similar way to Ember computed property macros to return an entity or
 * entity list of the relationship type specified. This incorporates the computed decorator with
 * a dependent key so that our decorated property can be recalculated whenever the underlying
 * dependent data changes.
 * @param {DataModelName} entityType - name of the data model that specifies the entity type
 * @param {string} dependentKey - a key that the decorator may depend on for computed property
 *
 * @example
 * Usage example:
 * ```
 * class PersonEntity extends BaseEntity<{}> {
 *   ...
 *   directReportUrns: Array<string> = ['charmander', 'bulbasaur', 'squirtle'];
 *
 *   @relationship('people', 'directReportUrns')
 *   directReports!: Array<PersonEntity>;
 * }
 * ```
 */
export const relationship = function(entityType: DataModelName, dependentKey: string): PropertyDecorator {
  // Defines a getter function for us to use as our computed function to fetch the data model
  // instances
  function getEntityInstanceForDependentKey(
    this: IRelationshipDecoratedClassPrototype
  ): DataModelEntityInstance | Array<DataModelEntityInstance> | undefined {
    const getters: Partial<Record<DataModelName, DataModelsRelationshipGetter<DataModelEntityInstance>>> =
      this.__relationshipGetters || {};
    const getterForEntityType = getters[entityType];
    return getterForEntityType ? getterForEntityType(this[dependentKey] as string | Array<string>) : undefined;
  }

  // Gives us the MethodDecorator given by the @computed decorator from Ember
  const computedFn: MethodDecorator = computed(dependentKey, getEntityInstanceForDependentKey);

  // Note: Underlying Ember implementation for computed macros, which we are taking advantage of,
  return function(
    target: IRelationshipDecoratedClassPrototype,
    propertyKey: string,
    ...args: Array<PropertyDescriptor | string>
  ): void | PropertyDescriptor {
    // Ensure we're tracking what this entity has access to
    target.__relationships = target.__relationships || new Set();
    target.__relationships.add(entityType);
    const descriptor = args[0] as PropertyDescriptor;

    // Returning the computed function actually attaches the "computed-ness" and new getter to the class
    return computedFn(target, propertyKey, descriptor);
  };
};
