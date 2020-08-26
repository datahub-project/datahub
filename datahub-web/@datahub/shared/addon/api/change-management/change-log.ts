import { getJSON, postJSON, putJSON } from '@datahub/utils/api/fetcher';
import { ApiVersion } from '@datahub/utils/api/shared';
import { entityApiRoot, entityApiByUrn } from '@datahub/data-models/api/entity';

export const changeLogEndpoint = 'data-construct-change-managements';

/**
 * Api call to get the detail payload of a Change Management log
 * @param id the identifier of the Change Management log
 */
export const getChangeLog = (
  id: number
): Promise<Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement> =>
  getJSON<Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement>({
    url: entityApiByUrn(`${id}`, changeLogEndpoint)
  });

/**
 * Api call to create a Change Management log
 * @param changeLog the payload of the Change Management log
 */
export const createChangeLog = (
  changeLog: Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagementContent
): Promise<void> => postJSON<void>({ url: entityApiRoot(changeLogEndpoint, ApiVersion.v2), data: changeLog });

/**
 * Api call to update an Change Management log
 * @param id the identifier of the Change Management log
 * @param changeLog the payload of the Change Management log
 */
export const updateChangeLog = (
  id: number,
  changeLog: Com.Linkedin.DataConstructChangeManagement.DataConstructChangeManagement
): Promise<void> => putJSON<void>({ url: entityApiByUrn(`${id}`, changeLogEndpoint), data: changeLog });
