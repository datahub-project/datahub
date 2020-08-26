import { datasetPlatformRegExp } from '@datahub/metadata-types/utils/entity/dataset/platform/urn';

/**
 * Given a standardized platform urn, extracts the platform name, which is the last string preceded by a colon
 * If this is undefined, returns the original string value
 * @param {string} platform Standardized platform urn where a dataset is defined
 * @return string
 */
export const getPlatformNameFromPlatformUrn = (platform = ''): string => {
  const matchOrNull = datasetPlatformRegExp.exec(platform);

  if (matchOrNull) {
    const [, name] = matchOrNull;
    return name;
  }

  // fallback to platform if unable to match name
  return platform;
};
