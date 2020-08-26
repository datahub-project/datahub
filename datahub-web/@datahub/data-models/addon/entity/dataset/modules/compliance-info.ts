import { IDatasetComplianceInfo } from '@datahub/metadata-types/types/entity/dataset/compliance/info';
import DatasetComplianceAnnotation from '@datahub/data-models/entity/dataset/modules/compliance-annotation';
import {
  IEntityComplianceSuggestion,
  SuggestionSource
} from '@datahub/metadata-types/constants/entity/dataset/compliance-suggestion';
import DatasetComplianceSuggestion from '@datahub/data-models/entity/dataset/modules/compliance-suggestion';
import { IComplianceFieldAnnotation } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-annotation';
import { set, setProperties } from '@ember/object';
import { decodeUrn } from '@datahub/utils/validators/urn';
import { isArray } from '@ember/array';
import { oneWay } from '@ember/object/computed';
import { PurgePolicy } from '@datahub/metadata-types/constants/entity/dataset/compliance/purge-policy';
import {
  IFieldAnnotationDiff,
  analyzeAnnotationsDiff
} from '@datahub/data-models/entity/dataset/helpers/validators/compliance/analyze-diff';
import { computed } from '@ember/object';
import { applyAnnotationsByEditableProperties } from '@datahub/data-models/entity/dataset/modules/constants/annotation-helpers';
import DatasetSchema from '@datahub/data-models/entity/dataset/modules/schema';
import { filterAnnotationsToDatasetSchema } from '@datahub/data-models/entity/dataset/helpers/validators/compliance/schema';
import { omit } from 'lodash';
import { IComplianceAnnotationUpdates } from '@datahub/data-models/types/entity/dataset';

interface IReadWorkingComplianceOptions {
  withoutNullFields?: boolean;
  schema?: DatasetSchema;
}

/**
 * Builds a default shape for securitySpecification & privacyCompliancePolicy with default / unset values
 *   for non null properties as per Avro schema
 * @param {string} datasetUrn identifier for the dataset that this privacy object applies to
 * @return {IComplianceInfo}
 */
export const initialComplianceObjectFactory = (datasetUrn: string): IDatasetComplianceInfo => ({
  datasetUrn: decodeUrn(datasetUrn),
  datasetId: null,
  confidentiality: null,
  complianceType: '',
  compliancePurgeNote: '',
  complianceEntities: [],
  datasetClassification: null
});

/**
 * Strips out the readonly attribute from a list of compliance annotations
 * @param {Array<IComplianceFieldAnnotation>} tags list of compliance annotations for a specific entity e.g dataset
 */
export const removeReadonlyAttributeFromTags = (
  tags: Array<IComplianceFieldAnnotation>
): Array<Omit<IComplianceFieldAnnotation, 'readonly'>> =>
  tags.map((tag: IComplianceFieldAnnotation): Omit<IComplianceFieldAnnotation, 'readonly'> => omit(tag, ['readonly']));

/**
 * Filters out compliance annotations that are not editable from a supplied list
 * @param {Array<IComplianceFieldAnnotation>} tags list of compliance annotations for a specific entity e.g dataset
 */
export const getEditableTags = (tags: Array<IComplianceFieldAnnotation>): Array<IComplianceFieldAnnotation> =>
  tags.filter(({ readonly }: IComplianceFieldAnnotation): boolean => !readonly);

/**
 * Filters out compliance annotations that do not have a value supplied for the annotation attributes identifierField and identifierType
 * @param {Array<IComplianceFieldAnnotation>} tags list of compliance annotations for a specific entity e.g dataset
 */
export const removeTagsMissingIdentifierAttributes = (
  tags: Array<IComplianceFieldAnnotation>
): Array<IComplianceFieldAnnotation> =>
  tags.filter((tag: IComplianceFieldAnnotation): boolean => !!tag.identifierField && !!tag.identifierType);

/**
 * Each dataset is expected to have info related to the compliance aspects, including things such as annotations
 * as well as suggested compliance changes. This class holds each of these fields and helps to facilitate the
 * information related to that area for the dataset. When this class is initialized, it takes a lot of the
 * compliance raw data and maps to our decorated classes to have more information available to our components
 */
export default class DatasetComplianceInfo {
  /**
   * The original data that initialized this class. Stored here and expected to remain static. Allows us to
   * maintain the original data read from the API. The initialization of this class is not connected to the
   * API for each individual tag, so this property does not need an async getter
   * @type {IDatasetComplianceInfo}
   */
  private readonly data: IDatasetComplianceInfo;

  /**
   * A dataset is expected to have a number of fields that can be tagged with metadata regarding what kind of
   * information that dataset field contains (namely in terms of PII).
   * @type {Array<DatasetComplianceAnnotation>}
   */
  annotations!: Array<DatasetComplianceAnnotation>;

  /**
   * Raw data from the compliance suggestions api call for this particular dataset
   * @type {Array<IEntityComplianceSuggestion>}
   */
  rawSuggestions!: Array<IEntityComplianceSuggestion>;

  /**
   * A dataset is expected to have system suggestions provided for its various fields, which in turn need to have
   * some decorated information in order to provide a readable view.
   * @type {Array<DatasetComplianceSuggestion>}
   */
  readonly suggestions: Array<DatasetComplianceSuggestion>;

  /**
   * If the dataset returns not found for compliance, we assume that this dataset does not yet have any compliance
   * info and therefore create it from a UI generated factory and mark this as new compliance info
   * @type {boolean}
   */
  isNewComplianceInfo = false;

  /**
   * Readonly property that lets us know based on the received data who was the last person to modify  the compliance
   * info
   * @type {string}
   */
  @oneWay('data.modifiedBy')
  modifiedBy?: string;

  /**
   * Readonly property accessed from the data object that lets us know the unix timestamp of the last time the
   * compliance info was modified
   * @type {number}
   */
  @oneWay('data.modifiedTime')
  modifiedTime?: number;

  /**
   * Accessed from the class data, returns the Purge Policy fetched from the dataset for this compliance info
   * @type {PurgePolicy}
   */
  @oneWay('data.complianceType')
  complianceType?: PurgePolicy;

  /**
   * Accessed from the class data, returns whether or not the compliance info was retrieved from an upstream dataset
   * @type {boolean}
   */
  @oneWay('data.fromUpstream')
  fromUpstream?: boolean;

  /**
   * Accessed from the class data, returns whether or not the dataset contains personal information
   * @type {boolean | null}
   */
  @oneWay('data.containingPersonalData')
  containsPersonalData?: boolean | null;

  /**
   * Reads the original annotations read from the api layer, but as their classified dataset compliance annotation
   * object form
   * @type {Array<DatasetComplianceAnnotation>}
   */
  @computed('data')
  get readOriginalAnnotations(): Array<DatasetComplianceAnnotation> {
    const { complianceEntities = [] } = this.data;
    return complianceEntities.map(
      (annotation: IComplianceFieldAnnotation): DatasetComplianceAnnotation =>
        new DatasetComplianceAnnotation(annotation)
    );
  }

  /**
   * Returns where we resolved the compliance info from (which dataset urn) which may match up to the dataset
   * entity or come from something upstream
   */
  @computed('data.datasetUrn')
  get resolvedFrom(): string | undefined {
    return this.data.datasetUrn;
  }

  /**
   * Working copy of the confidentiality classification for this dataset compliance
   */
  // Note: Asserting as definitely defined as we set this in createWorkingCopy() called in the constructor
  confidentiality!: IDatasetComplianceInfo['confidentiality'];

  /**
   * Working copy of the flag for whether or not the underlying dataset for this compliance
   * contains any person data
   */
  // Note: Asserting as definitely defined as we set this in createWorkingCopy() called in the constructor
  containingPersonalData!: IDatasetComplianceInfo['containingPersonalData'];

  /**
   * In any scenario where we need to restore all parts of our "working copy" for compliance, this is a useful
   * method to call. It will also be used in the constructor to initialize our working copy
   */
  createWorkingCopy(): void {
    const { complianceEntities = [], confidentiality, containingPersonalData } = this.data;
    setProperties(this, {
      confidentiality,
      containingPersonalData,
      annotations: complianceEntities.map(
        (annotation: IComplianceFieldAnnotation): DatasetComplianceAnnotation =>
          new DatasetComplianceAnnotation(annotation)
      )
    });
  }

  /**
   * When we are about to make a post request, we can use this function to actually read the working annotations
   * so that the classified information can be returned as a format expected by the api layer
   */
  readWorkingCopy({ withoutNullFields, schema }: IReadWorkingComplianceOptions): IDatasetComplianceInfo {
    // Prior to saving the updated annotations, extract only editable annotations i.e. annotations that do no have the readonly flag
    // set to true, and omit the readonly attribute from the annotations to be sent to the endpoint
    const tags = removeReadonlyAttributeFromTags(getEditableTags(this.getWorkingAnnotations(schema)));
    const complianceEntities = withoutNullFields ? removeTagsMissingIdentifierAttributes(tags) : tags;
    const { data, confidentiality, containingPersonalData } = this;

    return {
      ...data,
      complianceEntities,
      confidentiality,
      containingPersonalData
    };
  }

  /**
   * Adds an annotation to our working copy. Note that this does not affect the original annotations data. Uses
   * replacement instead of mutation to avoid unintended effects
   * @param {DatasetComplianceAnnotation | Array<DatasetComplianceAnnotation>} annotationsToBeAdded - the working copy of
   * the annotation / annotations to be added
   */
  addAnnotation(
    annotationsToBeAdded: DatasetComplianceAnnotation | Array<DatasetComplianceAnnotation>
  ): Array<DatasetComplianceAnnotation> {
    return set(this, 'annotations', this.annotations.concat(annotationsToBeAdded));
  }

  /**
   * Removes an annotation from our working copy only. Uses replacement instead of mutation to avoid unintended
   * effects.
   * @param {(DatasetComplianceAnnotation | Array<DatasetComplianceAnnotation>)} annotations annotations or single annotation to be removed from the compliance policy
   */
  removeAnnotation(
    annotations: DatasetComplianceAnnotation | Array<DatasetComplianceAnnotation>
  ): Array<DatasetComplianceAnnotation> {
    // Ensure the annotationsToBeRemoved is manipulated as a list even if a single item may be supplied
    const annotationsToBeRemoved = ([] as Array<DatasetComplianceAnnotation>).concat(annotations);
    // For identifierType and logicalType are nullable values on IComplianceFieldAnnotation however, non-nullable on
    // Com.Linkedin.Dataset.FieldCompliance which is the backing datatype for proposals. Null is added below to account for this
    // and accurately compare values
    const matchRemovedAnnotationOn = {
      identifierField: annotationsToBeRemoved.mapBy('identifierField'),
      identifierType: annotationsToBeRemoved.mapBy('identifierType').concat(null),
      logicalType: annotationsToBeRemoved.mapBy('logicalType').concat(null)
    };
    const updatedAnnotations = this.annotations.filter(
      (annotation: DatasetComplianceAnnotation): boolean =>
        !(
          matchRemovedAnnotationOn.identifierField.includes(annotation.identifierField) &&
          matchRemovedAnnotationOn.identifierType.includes(annotation.identifierType) &&
          matchRemovedAnnotationOn.logicalType.includes(annotation.logicalType)
        )
    );

    return set(this, 'annotations', updatedAnnotations);
  }

  /**
   * This allows us to apply the annotations given by the user for a specific field and overwrite all the current
   * tags for that specific field. We do so by first making sure all fields to overwrite are valid, and then do a
   * "replace" by filtering out any current annotations for the field from our list and adding in the diff.
   * @param {string} fieldName - identifies the identifierField we are writing
   * @param {Array<DatasetComplianceAnnotation>} fieldDiff - new annotations to add/overwrite for current field
   * @returns {boolean} signifying if this apply process was successful
   */
  applyAnnotationsByField(fieldName: string, fieldDiff: Array<DatasetComplianceAnnotation>): boolean {
    if (!fieldName || !isArray(fieldDiff)) {
      return false;
    }

    const allFieldDiffsAreValid = fieldDiff.reduce(
      (result: boolean, diff: DatasetComplianceAnnotation): boolean => result && diff.isValidAnnotation(),
      true
    );

    if (!allFieldDiffsAreValid) {
      return false;
    }

    const { annotations } = this;
    const annotationsForOtherFields = annotations.filter(
      (tag: DatasetComplianceAnnotation): boolean => tag.identifierField !== fieldName
    );

    set(this, 'annotations', annotationsForOtherFields.concat(fieldDiff));
    return true;
  }

  /**
   * Creates a list of "diff" objects, each representing a field in the dataset schema that will describe the
   * differences between the original annotations read from the API and the current modified working copy for
   * that particular field
   */
  getWorkingCopyDiff(): Array<IFieldAnnotationDiff> {
    return analyzeAnnotationsDiff(this.readOriginalAnnotations, this.annotations);
  }

  /**
   * Modifies the working copy for our confidentiality classification for the dataset
   * @param {DatasetClassification} newConfidentiality - new confidentiality classification to modify our working copy
   */
  updateWorkingConfidentiality(newConfidentiality: IDatasetComplianceInfo['confidentiality']): void {
    set(this, 'confidentiality', newConfidentiality);
  }

  /**
   * Modifies the working copy for our personal data flag for the dataset
   * @param {boolean} newContainingPersonalDataFlag - new personal data flag indication
   */
  updateWorkingContainingPersonalData(
    newContainingPersonalDataFlag: IDatasetComplianceInfo['containingPersonalData']
  ): void {
    set(this, 'containingPersonalData', newContainingPersonalDataFlag);
  }

  /**
   * Get flattened lists of dataset compliance annotations that have been added, deleted, changed
   * add / removed also includes changed annotations (add+delete)
   */
  getAnnotationUpdates(): IComplianceAnnotationUpdates {
    /**
     * Extract reference to the compliance annotation instance
     * @param {{ annotation: DatasetComplianceAnnotation }} { annotation } object (IDiffValue), containing a DatasetComplianceAnnotation
     */
    const getAnnotation = ({ annotation }: { annotation: DatasetComplianceAnnotation }): DatasetComplianceAnnotation =>
      annotation;

    return this.getWorkingCopyDiff().reduce(
      (flatAddedOrRemovedAnnotations, { newValues, oldValues, mutations }): IComplianceAnnotationUpdates => ({
        added: [...flatAddedOrRemovedAnnotations.added, ...newValues.map(getAnnotation)],
        removed: [...flatAddedOrRemovedAnnotations.removed, ...oldValues.map(getAnnotation)],
        onlyChanged: [...flatAddedOrRemovedAnnotations.onlyChanged, ...mutations.map(getAnnotation)]
      }),
      { added: [], removed: [], onlyChanged: [] }
    );
  }

  /**
   * Calls each annotation's working copy to return a final list of all the annotation working copies but in the
   * format expected by our API layer. This can be passed back to the API already massaged to meet the endpoint's
   * expectations
   */
  getWorkingAnnotations(schema?: DatasetSchema): Array<IComplianceFieldAnnotation> {
    const filteredAnnotations = schema ? filterAnnotationsToDatasetSchema(this.annotations, schema) : this.annotations;
    return filteredAnnotations.map(
      (annotation: DatasetComplianceAnnotation): IComplianceFieldAnnotation => annotation.readWorkingCopy()
    );
  }

  /**
   * Given a list of objects created from just the editable parts of compliance annotation tags, create a list
   * of compliance annotation tag class objects that we can gleam a proper working copy from
   * @param workingEditableProps - array of objects created with just the editable props in annotation tags
   */
  overrideAnnotationsByEditableProps(workingEditableProps: Array<unknown>): Array<DatasetComplianceAnnotation> {
    return set(this, 'annotations', applyAnnotationsByEditableProperties(workingEditableProps));
  }

  constructor(
    complianceInfo: IDatasetComplianceInfo,
    suggestions: Array<IEntityComplianceSuggestion>,
    isNewComplianceInfo?: boolean
  ) {
    this.data = complianceInfo;
    this.rawSuggestions = suggestions;
    this.suggestions = suggestions.map(
      (suggestion: IEntityComplianceSuggestion): DatasetComplianceSuggestion =>
        new DatasetComplianceSuggestion(suggestion, SuggestionSource.system)
    );
    this.isNewComplianceInfo = !!isNewComplianceInfo;

    this.createWorkingCopy();
  }
}
