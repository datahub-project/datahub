import Application from '../app';
import config from '../config/environment';
import { setApplication } from '@ember/test-helpers';
import { start } from 'ember-qunit';
import { registerDeprecationHandler } from '@ember/debug';

// Suggestion to keep this here until we are ready to handle the deprecation to use definePRoperty for
// computed properties. Otherwise, tests and build logs becoming overwhelming to look at
registerDeprecationHandler(function(message, { id }, next) {
  if (!message.includes('defineProperty')) {
    next(...arguments);
  }
});

setApplication(Application.create(config.APP));

start();
