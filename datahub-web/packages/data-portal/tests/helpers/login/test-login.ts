import { visit, click, fillIn } from '@ember/test-helpers';
import { authenticationUrl, testUser, testPassword, testVipToken } from 'wherehows-web/tests/helpers/login/constants';
import {
  loginUserInput,
  loginPasswordInput,
  loginSubmitButton,
  loginVipTokenInput
} from 'wherehows-web/tests/helpers/login/page-element-constants';

const appLogin = async (): Promise<void> => {
  await visit(authenticationUrl);
  await fillIn(loginUserInput, testUser);
  await fillIn(loginPasswordInput, testPassword);
  await fillIn(loginVipTokenInput, testVipToken.toString());
  await click(loginSubmitButton);
};

export default appLogin;
