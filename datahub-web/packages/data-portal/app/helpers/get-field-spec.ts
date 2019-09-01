import { helper } from '@ember/component/helper';
import { findInArray } from 'wherehows-web/helpers/find-in-array';
import { ISearchEntityRenderProps } from '@datahub/data-models/types/entity/rendering/search-entity-render-prop';

/**
 * Quick helper that leverages another helper to specifically find a field spec by name from
 * an entity definition's field specs
 * @param {Array<ISearchEntityRenderProps>} fieldSpecs - an entity definition's field specs
 * @param {string} field - name of the field to look for
 * @returns {ISearchEntityRenderProps}
 */
// TODO: This helper should be migrated out of Wherehows whenever we move to abstract entity definitions
export function getFieldSpec([fieldSpecs, field]: [Array<ISearchEntityRenderProps>, string]):
  | ISearchEntityRenderProps
  | undefined {
  return findInArray<ISearchEntityRenderProps>([
    fieldSpecs,
    (spec: ISearchEntityRenderProps) => spec.fieldName === field
  ]);
}

export default helper(getFieldSpec);
