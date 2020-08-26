import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import layout from '../templates/components/test-pwr-lookup';
import { classNames } from '@ember-decorators/component';

@classNames('test-pwr-lookup')
export default class TestPwrLookup extends Component {
  layout = layout;

  fakeSearchHandler(query: string, _syncResults: () => void, asyncResults: (results: Array<string>) => void): void {
    const myEntities = ['pikachu', 'charmander', 'squirtle', 'bulbasaur'];

    asyncResults(myEntities.filter(entity => new RegExp(`^${query}.*`, 'i').test(entity)));
  }

  fakeResultHandler(result: string): string {
    return result;
  }
}
