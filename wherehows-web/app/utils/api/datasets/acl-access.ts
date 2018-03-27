import { notFoundApiError, serverExceptionApiError } from 'wherehows-web/utils/api';
import { datasetUrlByUrn } from 'wherehows-web/utils/api/datasets/shared';
import { deleteJSON, getJSON, putJSON } from 'wherehows-web/utils/api/fetcher';
import {
  IAccessControlEntry,
  IGetAclsResponse,
  IRequestAccessControlEntry
} from 'wherehows-web/typings/api/datasets/aclaccess';

/**
 * Returns the dataset acls url by urn
 * @param {string} urn the urn for the dataset
 * @return {string}
 */
const datasetAclsUrlByUrn = (urn: string): string => `${datasetUrlByUrn(urn)}/acl`;

/**
 * Gets the url for the adding an entry to the acl
 * @param {string} urn the urn for the dataset
 * @return {string}
 */
const addDatasetAclUrnByUrn = (urn: string): string => `${datasetAclsUrlByUrn(urn)}/add`;

/**
 * Gets the url to remove the current user from the acl
 * @param {string} urn datasets urn
 * @return {string}
 */
const deleteDatasetAclUrnByUrn = (urn: string): string => `${datasetAclsUrlByUrn(urn)}/remove`;

/**
 * Requests access to the acl for the currently logged in user
 * @param {string} urn dataset urn
 * @param {IRequestAccessControlEntry} props
 * @return {Promise<void>}
 */
const requestAclAccess = (urn: string, props: IRequestAccessControlEntry): Promise<void> => {
  try {
    return putJSON<void>({ url: addDatasetAclUrnByUrn(urn), data: props });
  } catch (e) {
    if (serverExceptionApiError(e)) {
      // TODO: retry with exponential back-off
    }

    throw e;
  }
};

/**
 * Removes access to the acl for the currently logged in user
 * @param {string} urn
 * @return {Promise<void>}
 */
const removeAclAccess = (urn: string): Promise<void> => {
  try {
    return deleteJSON<void>({ url: deleteDatasetAclUrnByUrn(urn) });
  } catch (e) {
    if (serverExceptionApiError(e)) {
      // TODO: retry with exponential back-off
    }

    throw e;
  }
};

/**
 * Gets the list of current acl entries
 * @param {string} urn
 * @return {Promise<Array<IAccessControlEntry>>}
 */
const readDatasetAcls = async (urn: string): Promise<Array<IAccessControlEntry>> => {
  let acls: Array<IAccessControlEntry> = [];

  try {
    return getJSON<IGetAclsResponse>({ url: datasetAclsUrlByUrn(urn) });
  } catch (e) {
    if (notFoundApiError(e)) {
      return acls;
    }

    throw e;
  }
};

export { requestAclAccess, removeAclAccess, readDatasetAcls };
