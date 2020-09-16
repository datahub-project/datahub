import Route from '@ember/routing/route';
import { Pokemon } from '../models/pokemon';

export default class Wiki extends Route {
  model(): { testEntity: Pokemon } {
    return {
      testEntity: new Pokemon('pikachu:urn')
    };
  }
}
