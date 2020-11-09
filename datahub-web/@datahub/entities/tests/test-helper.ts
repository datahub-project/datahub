import Application from 'dummy/app';
import config from '../config/environment';
import { setApplication } from '@ember/test-helpers';
import { start } from 'ember-qunit';
import 'qunit-dom';

setApplication(Application.create(config.APP));

start();
