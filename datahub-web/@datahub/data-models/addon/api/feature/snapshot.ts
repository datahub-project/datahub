import { IFeatureSnapshot } from '@datahub/metadata-types/types/metadata/feature-snapshot';
import { getJSON } from '@datahub/utils/api/fetcher';
import { featureUrlByUrn } from '@datahub/data-models/api/feature/feature';
import { featureUrlRoot } from '@datahub/data-models/api/feature';
import { ApiVersion } from '@datahub/utils/api/shared';

/**
 * Constructs the Feature instance snapshot url from the urn
 * @param {string} urn string urn value for the feature snapshot required
 * @returns {string}
 */
const featureSnapshotUrlByUrn = (urn: string): string => `${featureUrlByUrn(urn)}/snapshot`;

/**
 * Queries the endpoint at the url to request feature snapshot information
 * @param {string} urn
 * @returns {Promise<IFeatureSnapshot>}
 */
export const readFeatureSnapshot = (urn: string): Promise<IFeatureSnapshot> =>
  getJSON({ url: featureSnapshotUrlByUrn(urn) });

/**
 * Constructs the url for the batch GET endpoint for Feature Snapshots
 */
const featureSnapshotsUrl = (urns: Array<string>): string =>
  `${featureUrlRoot(ApiVersion.v2)}/snapshots/${urns.join(';')}`;

/**
 * Reads the list of snapshots at the batch GET endpoint for Feature Entities
 */
export const readFeatureSnapshots = (urns: Array<string>): Promise<Array<IFeatureSnapshot>> =>
  getJSON<Array<IFeatureSnapshot>>({ url: featureSnapshotsUrl(urns) });
