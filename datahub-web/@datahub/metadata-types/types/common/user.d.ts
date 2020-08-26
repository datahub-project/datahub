/**
 * Describes the interface for the user json returned
 * from the current user endpoint
 * https://jarvis.corp.linkedin.com/codesearch/result/?name=User.java&path=wherehows-frontend%2Fdatahub-dao%2Fsrc%2Fmain%2Fjava%2Fcom%2Flinkedin%2Fdatahub%2Fmodels%2Ftable&reponame=wherehows%2Fwherehows-frontend#User
 */
export interface IUser {
  departmentNum: number;
  email: string;
  id: number;
  name: string;
  userName: string;
  userSetting: null | {
    defaultWatch: string;
    detailDefaultView: string;
  };
}
