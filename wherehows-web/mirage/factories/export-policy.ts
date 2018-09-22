import { Factory } from 'ember-cli-mirage';

const randomize = () => Math.random() < 0.5;

export default Factory.extend({
  containsUserActionGeneratedContent() {
    return this.randomized ? randomize() : false;
  },

  containsUserDerivedContent() {
    return this.randomized ? randomize() : false;
  },

  containsUserGeneratedContent() {
    return this.randomized ? randomize() : false;
  }
});
