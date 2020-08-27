import Controller from '@ember/controller';
import { AppName } from '@datahub/shared/constants/global';

export default class ApplicationError extends Controller {
  /**
   * Name of the app being displayed on the Error page.
   */
  appName: string = AppName;
}
