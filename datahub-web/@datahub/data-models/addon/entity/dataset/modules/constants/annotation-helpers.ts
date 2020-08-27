import DatasetComplianceAnnotation from '@datahub/data-models/entity/dataset/modules/compliance-annotation';
import {
  ComplianceFieldIdValue,
  MemberIdLogicalType
} from '@datahub/metadata-types/constants/entity/dataset/compliance-field-types';
import {
  ComplianceAnnotationsEditableProps,
  RawComplianceAnnotationsEditableProps
} from '@datahub/data-models/entity/dataset/helpers/validators/compliance/annotations';
import { setProperties } from '@ember/object';
import { IComplianceFieldAnnotation } from '@datahub/metadata-types/constants/entity/dataset/compliance-field-annotation';
import { pick } from 'lodash';
import { IComplianceDataType } from '@datahub/metadata-types/types/entity/dataset/compliance-data-types';

/**
 * Helper utility function used to determine whether an annotation tag is currently valid. Especially
 * will be helpful in cases such as the annotation selection where if the user has created a valid
 * (and therefore complete) tag we want to close the selection window instead of making them have to
 * press a specific "done" or "save" button
 * @param {DatasetComplianceAnnotation} annotationTag - the tag whose context is being used for the
 *  validity check
 * @param {Array<IComplianceDataType>} complianceDataTypes - optional, if provided a data source for
 *  compliance data types then we use this as a more reliable way to determine if the annotation is
 *  valid or not than our frontend hardcoded logic
 */
export const annotationIsValid = (
  annotationTag: DatasetComplianceAnnotation,
  complianceDataTypes?: Array<IComplianceDataType>
): boolean => {
  const { identifierType, logicalType, isPurgeKey, valuePattern } = annotationTag;
  const memberIdTypes = Object.values(ComplianceFieldIdValue);
  const typesWithSupportedFieldFormats = (complianceDataTypes || [])
    .filter((dataType: IComplianceDataType): boolean => dataType.supportedFieldFormats.length > 0)
    .map((dataType: IComplianceDataType): string => dataType.id);

  // identifierType is a required field, so if it doesn't exist nothing else matters
  if (!identifierType) {
    return false;
  }

  const shouldIncludeLogicalType =
    (memberIdTypes.includes(identifierType as ComplianceFieldIdValue) &&
      identifierType !== ComplianceFieldIdValue.None) ||
    typesWithSupportedFieldFormats.includes(identifierType);

  // If we have a memberId field (excluding None from this check), then we should have a logical type and
  // determination of whether the field tag is a purge key. Otherwise, we are not valid
  if (shouldIncludeLogicalType) {
    if (!logicalType || typeof isPurgeKey !== 'boolean') {
      return false;
    }
    // If we have a custom type declared, then we need to have a regex entered for this
    if (logicalType === MemberIdLogicalType.Custom && !valuePattern) {
      return false;
    }
  }
  // Truthy assumption based on not failing any of the above tests
  return true;
};

/**
 * Shortcut typing so that we can refer to a subset of DatasetComplianceAnnotation of only editable props
 */
type AnnotationEditablePropsOnly = Pick<DatasetComplianceAnnotation, ComplianceAnnotationsEditableProps>;

/**
 * Shortcut to typing so that we can refer to a subset of the raw API response for compliance
 * IComplianceFieldAnnotation with only its editable props
 */
type RawAnnotationEditablePropsOnly = Pick<IComplianceFieldAnnotation, RawComplianceAnnotationsEditableProps>;

/**
 * Uses the nature of setProperties to apply all the editable props found within the props parameter to
 * the annotation passed into this function
 * @param annotation - annotation to have props applied to
 * @param props - props to be applied to the annotation
 */
const applyEditablePropsToAnnotation = (
  annotation: DatasetComplianceAnnotation,
  props: RawAnnotationEditablePropsOnly
): DatasetComplianceAnnotation => {
  // What we are doing here is converting the properties that are found in the raw annotations and transforming them
  // into the classified format and then restricting that down to only the editable properties to use to override
  // the existing annotation with its editable props
  const newAnnotationWithProps = new DatasetComplianceAnnotation({
    ...props,
    pii: false,
    securityClassification: null
  });
  const newAnnotationAsEditableProps: AnnotationEditablePropsOnly = pick(
    newAnnotationWithProps,
    DatasetComplianceAnnotation.modifiableKeysOnClassData()
  );

  setProperties(annotation, newAnnotationAsEditableProps);
  return annotation;
};

/**
 * From a working copy of only editable props, we create new tags and apply these values so that we can
 * create a new working copy annotations list out of them
 * @param newWorkingCopy - new working copy of only editable props that we want to apply to new tags
 */
export const applyAnnotationsByEditableProperties = (
  newWorkingCopy: Array<unknown>
): Array<DatasetComplianceAnnotation> =>
  newWorkingCopy.map(
    (item: RawAnnotationEditablePropsOnly): DatasetComplianceAnnotation =>
      applyEditablePropsToAnnotation(new DatasetComplianceAnnotation(undefined, item.identifierField), item)
  );
