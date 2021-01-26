/**
 * The expected response for the the Group entity api call from the backend.
 */
export interface ICorpGroupResponse {
  name: string;
  info: Com.Linkedin.Identity.CorpGroupInfo;
  urn: string;
}
