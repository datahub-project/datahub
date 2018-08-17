import Service from '@ember/service';
import { IObject } from 'wherehows-web/typings/generic';
import { get } from '@ember/object';

export default class DatasetMeta extends Service {
  healthScore: number;

  healthScores: IObject<number> = {};

  setHealthScoreForUrn(urn: string, score: number) {
    get(this, 'healthScores')[urn] = score;
  }
}

// DO NOT DELETE: this is how TypeScript knows how to look up your services.
declare module '@ember/service' {
  interface Registry {
    'dataset-meta': DatasetMeta;
  }
}
