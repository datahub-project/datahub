import Service from '@ember/service';
import users from 'wherehows-web/mirage/fixtures/users';

const [user] = users;

export default class extends Service {
  currentUser = user;
  load = () => Promise.resolve();
  invalidateSession = () => {};
  trackCurrentUser = () => {};
}
