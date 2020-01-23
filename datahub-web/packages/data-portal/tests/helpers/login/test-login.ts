import { visit, click, fillIn } from '@ember/test-helpers';
import { authenticationUrl, testUser, testPassword } from 'wherehows-web/tests/helpers/login/constants';
import {
  loginUserInput,
  loginPasswordInput,
  loginSubmitButton
} from 'wherehows-web/tests/helpers/login/page-element-constants';

const appLogin = async (): Promise<void> => {
  await visit(authenticationUrl);
  await fillIn(loginUserInput, testUser);
  await fillIn(loginPasswordInput, testPassword);
  await click(loginSubmitButton);
};

export default appLogin;
