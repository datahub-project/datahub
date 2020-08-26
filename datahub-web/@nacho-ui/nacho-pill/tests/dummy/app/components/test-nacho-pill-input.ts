import Component from '@ember/component';
// @ts-ignore: Ignore import of compiled template
import template from '../templates/components/test-nacho-pill-input';
import { layout, classNames } from '@ember-decorators/component';
import { action } from '@ember/object';
import { A } from '@ember/array';

@layout(template)
@classNames('test-pill-input')
export default class TestNachoPillInput extends Component {
  tagList = A(['pikachu', 'squirtle', 'bulbasaur']);

  maxlength = 5;

  @action
  addTag(newTag: string): void {
    this.tagList.addObject(newTag);
  }

  @action
  removeTag(tag: string): void {
    this.tagList.removeObject(tag);
  }
}
