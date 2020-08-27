import Route from '@ember/routing/route';

export default class Demo extends Route {
  model(): {
    queryparam: {
      pokemon: string;
    };
  } {
    return {
      queryparam: {
        pokemon: 'eevee'
      }
    };
  }
}
