import Controller from '@ember/controller';

export default class Demo extends Controller {
  queryParams = ['pokemon'];

  pokemon = '';
}
