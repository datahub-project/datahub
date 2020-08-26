import DatasetComplianceAnnotation from '@datahub/data-models/entity/dataset/modules/compliance-annotation';
import { groupBy, uniq } from 'lodash';
import { annotationsMatchByRelevantKeys } from '@datahub/data-models/entity/dataset/helpers/validators/compliance/comparators';

/**
 * Options for the type of diff we are defining, whether that diff type is a new item added (create), a modification
 * of an existing one (update) or a removal of one (delete)
 */
export enum DiffType {
  CREATE = 'create',
  UPDATE = 'update',
  DELETE = 'delete'
}

/**
 * Represents a single diff item in a list for
 */
export interface IDiffValue {
  type: DiffType;
  annotation: DatasetComplianceAnnotation;
}

/**
 * Expected object to describe the accumulated diffs for a single field in the compliance annotations
 */
export interface IFieldAnnotationDiff {
  identifierField: string;
  // Old annotations that are not deleted for the field are listed in oldValues, however,
  // this will include mutations since they are also a deletion of an old annotation and replacement with a new annotation
  // This can be refactored to address only deletions exclusive of mutations, downstream consumers will have to be updated as well
  oldValues: Array<IDiffValue>;
  // New annotations for the field are listed in newValues, however,
  // this is more encompassing list that will also include mutations since they are technically new values
  // This can be refactored to address only new additions exclusive of mutations, downstream consumers will have to be updated as well
  newValues: Array<IDiffValue>;
  // Mutations represent updates to existing annotations for a given field, type will be update
  mutations: Array<IDiffValue>;
}

/**
 * Creates an IFieldAnnotationDiff object for a particular given field. This will operate on the following assumptions:
 * Rule 1: If there are no annotations for a given field in the original, then everything that the user has done is
 *  presumed to be a create operation
 * Rule 2: If there are no annotations for a given field in the working copy, then everything that the user has done is
 *  presumed to be a delete operation
 * Rule 3: The more complicated rule, in a more complex scenario we might decide to detect what annotations are considered
 *  updates and what are adds and deletes, but we are seeking a more simplified version where anything in the original that
 *  is not in the working is a delete and everything in the working that is not in the original is an add. This means we
 *  need to determine fields that are diffs by removing fields that exist on both sides
 * @param {string} field - name of the field we are diffing for
 * @param {Array<DatasetComplianceAnnotation>} originalAnnotations - the original annotations for that field
 * @param {Array<DatasetComplianceAnnotation>} annotationsBeingUpdated - the working copy of annotations for that field after user changes
 */
const createAnnotationDiffForField = (
  field: string,
  originalAnnotations: Array<DatasetComplianceAnnotation> = [],
  annotationsBeingUpdated: Array<DatasetComplianceAnnotation> = []
): IFieldAnnotationDiff => {
  // Rule 1: If there are no original annotations for a certain field, then we know that anything in the
  // working copy is a CREATE type diff
  if (!originalAnnotations.length) {
    return {
      identifierField: field,
      oldValues: [],
      newValues: annotationsBeingUpdated.map(tag => ({ type: DiffType.CREATE, annotation: tag })),
      mutations: []
    };
  }

  // Rule 2: On the opposite end, if there are no working copy annotations for a particular field, then we
  // know that everything in the original was a delete for that field
  if (!annotationsBeingUpdated.length) {
    return {
      identifierField: field,
      oldValues: originalAnnotations.map(tag => ({ type: DiffType.DELETE, annotation: tag })),
      newValues: [],
      mutations: []
    };
  }

  // If both original and working copy annotations exist for a field, then there could be create, update,
  // or delete operations involved, or a combination of these
  // In the case of compliance annotations, we will call all diffs found in the original but not the
  // working copy are DELETE and all diffs found in the working copy but not original are CREATE
  // Any tag that is found in both copies is considered no diff

  /**
   * Creates a filter predicate that finds an annotation that is not present in the subsequently supplied
   * list of annotations using the function annotationsMatchByRelevantKeys for equality evaluation
   * @param {Array<DatasetComplianceAnnotation>} annotations the list of annotations to find where the relevant keys are matched i.e. annotationsMatchByRelevantKeys returns true for, but not wanted in the final list
   */
  const annotationsNotFoundIn = (
    annotations: Array<DatasetComplianceAnnotation>
  ): ((needle: DatasetComplianceAnnotation) => boolean) => (needle: DatasetComplianceAnnotation): boolean =>
    !annotations.find(annotation => annotationsMatchByRelevantKeys(annotation, needle));

  const updatedAnnotationsOnly = annotationsBeingUpdated.filter(annotationsNotFoundIn(originalAnnotations));
  const originalAnnotationsOnly = originalAnnotations.filter(annotationsNotFoundIn(annotationsBeingUpdated));

  // The current implementation will consider a mutation exclusively as a DiffType.DELETE & DiffType.CREATE,
  // 'mutations' provides an intersect between the
  return {
    identifierField: field,
    oldValues: originalAnnotationsOnly.map(tag => ({ type: DiffType.DELETE, annotation: tag })),
    newValues: updatedAnnotationsOnly.map(tag => ({ type: DiffType.CREATE, annotation: tag })),
    mutations: annotationsBeingUpdated
      .filter((annotation): boolean => annotation.hasChangedFromSavedAnnotation)
      .map((annotation): IDiffValue => ({ type: DiffType.UPDATE, annotation }))
  };
};

/**
 * Taking what we original fetched from the API and the working copy (accumulated user changes), we calculate a list of
 * of diff changes for every applicable field. This list can be used to power things such as any summary of changes report
 * for the user to see.
 * @param {Array<DatasetComplianceAnnotation>} original - original retrieved API data for the compliance info annotations that
 *  has been pre-converted into the compliance annotations classified object so that we can compare apples to apples
 * @param {Array<DatasetComplianceAnnotation} workingCopy - workingCopy created annotations
 */
export const analyzeAnnotationsDiff = (
  original: Array<DatasetComplianceAnnotation>,
  workingCopy: Array<DatasetComplianceAnnotation>
): Array<IFieldAnnotationDiff> => {
  // Groups the original annotations list by identifier field
  const originalAnnotationsGroupedByIdentifierField = groupBy(original, item => item.identifierField);
  // Groups the working copy annotations by identifier field. This way we can organize how we parse
  // through to find diffs
  const workingCopyGroupedByIdentifierField = groupBy(workingCopy, item => item.identifierField);

  // Create the list of identifier fields from each copy and then create a unique list of field names
  const originalAnnotationsFieldsList = Object.keys(originalAnnotationsGroupedByIdentifierField);
  const workingCopyFieldsList = Object.keys(workingCopyGroupedByIdentifierField);
  const uniqueFieldsList = uniq(originalAnnotationsFieldsList.concat(workingCopyFieldsList));

  // We use the unique field list to iterate through and seek out what kind of diffs exist for each field, if any
  const diffListPerField: Array<IFieldAnnotationDiff> = [];
  return uniqueFieldsList.reduce((diffList, field) => {
    const diffs = createAnnotationDiffForField(
      field,
      originalAnnotationsGroupedByIdentifierField[field],
      workingCopyGroupedByIdentifierField[field]
    );
    // We only add something if there are new values or old values to be compared. Otherwise, there
    // is no sense in adding in two empty lists (as this means no diff)
    if (diffs.newValues.length || diffs.oldValues.length) {
      diffList.push(diffs);
    }

    return diffList;
  }, diffListPerField);
};
