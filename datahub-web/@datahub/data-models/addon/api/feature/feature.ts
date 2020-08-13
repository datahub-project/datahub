import { IFeatureEntity } from '@datahub/metadata-types/types/entity/feature/feature-entity';
import { encodeUrn } from '@datahub/utils/validators/urn';
import { getJSON, postJSON } from '@datahub/utils/api/fetcher';
import { ApiVersion } from '@datahub/utils/api/shared';
import { featureUrlRoot } from '@datahub/data-models/api/feature';
import { IFrameFeatureStatusConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-status-config';
import { IFrameFeatureEditableConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-editable-config';
import { IFrameFeatureTierConfig } from '@datahub/metadata-types/types/entity/feature/frame/frame-feature-tier-config';

/**
 * Constructs the url for a Feature identified by the provided string urn
 * @param {string} urn the urn to use in querying for Feature entity
 * @returns {string}
 */
export const featureUrlByUrn = (urn: string): string => `${featureUrlRoot(ApiVersion.v2)}/${encodeUrn(urn)}`;

/**
 * Constructs the url to the Feature endpoint to update it's statusConfig metadata aspect
 * @param {string} urn the string urn for the Feature
 */
export const featureStatusConfigUrlByUrn = (urn: string): string => `${featureUrlByUrn(urn)}/statusconfig`;

/**
 * Constructs the url to the Feature endpoint to update it's tierConfig metadata aspect
 * @param {string} urn the string urn for the Feature Tier endpoint
 */
export const featureTierConfigUrlByUrn = (urn: string): string => `${featureUrlByUrn(urn)}/tierconfig`;
/**
 * Constructs the url to the Feature endpoint to update it's editableConfig metadata aspect
 * @param {string} urn the string urn for the Feature
 */
export const featureEditableConfigUrlByUrn = (urn: string): string => `${featureUrlByUrn(urn)}/editableconfig`;

/**
 * Queries the Feature endpoint with the urn provided to retrieve entity information
 * @param {string} urn
 * @returns {Promise<IFeatureEntity>}
 */
export const readFeature = (urn: string): Promise<IFeatureEntity> => getJSON({ url: featureUrlByUrn(urn) });

/**
 * Updates the Feature represented by the urn, by making a POST request to the endpoint at the url
 * @param {string} urn the string urn for the Feature to be updated
 * @param {IFrameFeatureStatusConfig} statusConfig the full statusConfig metadata aspect with updated values
 * @returns {Promise<{updated: true}>} if update is successful a response string with value of `updated` will be returned
 */
export const updateFeatureStatusConfig = (
  urn: string,
  statusConfig: IFrameFeatureStatusConfig
): Promise<{ updated: true }> => postJSON({ url: featureStatusConfigUrlByUrn(urn), data: statusConfig });

/**
 * Updates the Feature represented by the urn, by making a POST request to the endpoint at the url
 * @param {string} urn the string urn for the Feature to be updated
 * @param {IFrameFeatureEditableConfig} editableConfig the full editableConfig metadata aspect with the updated values for this Feature
 * @returns {Promise<{updated: true}>} if update is successful a response string with value of `updated` will be returned
 */
export const updateFeatureEditableConfig = (
  urn: string,
  editableConfig: IFrameFeatureEditableConfig
): Promise<{ updated: true }> => postJSON({ url: featureEditableConfigUrlByUrn(urn), data: editableConfig });

/**
 * Updates the Feature represented by the urn, by making a POST request to the endpoint at the url
 * @param {string} urn the string urn for the Feature to be updated
 * @param {IFrameFeatureTierConfig} tierConfig the tierConfig aspect with the updated value of `tier` for this Feature
 * @returns {Promise<{updated: true}>} if update is successful a response string with value of `updated` will be returned
 */
export const updateFeatureTierConfig = (urn: string, tierConfig: IFrameFeatureTierConfig): Promise<{ update: true }> =>
  postJSON({ url: featureTierConfigUrlByUrn(urn), data: tierConfig });
