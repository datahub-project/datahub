import { IHealthScore } from 'wherehows-web/typings/api/datasets/health';

export const healthCategories = [{ name: 'Compliance', value: 60 }, { name: 'Ownership', value: 40 }];
export const healthSeverity = [
  { name: 'Minor', value: 50 },
  { name: 'Warning', value: 30 },
  { name: 'Critical', value: 25 }
];
export const healthDetail = <Array<IHealthScore>>[
  {
    category: 'Compliance',
    description:
      'Sample description for this score based on compliance category that is extra long' +
      'just to make sure we can handle any length of descriptions',
    score: 30,
    severity: 'Critical'
  },
  {
    category: 'Compliance',
    description:
      'It is a dark time for the Rebellion. Although the Death Star has been destroyed, ' +
      'Imperial troops have driven the Rebel forces from their hidden base and pursued them across ' +
      'the galaxy.\nEvading the dreaded Imperial Starfleet, a group of freedom fighters led by Luke ' +
      'Skywalker has established a new secret base on the remote ice world of Hoth.\n' +
      'The evil lord Darth Vader, obsessed with finding young Skywalker, has dispatched thousands of ' +
      'remote probes into the far reaches of space...',
    score: 50,
    severity: 'Warning'
  },
  {
    category: 'Ownership',
    description: 'Sample ownership description here',
    score: 75,
    severity: 'Minor'
  },
  {
    category: 'Ownership',
    description: 'Sample ownership description here',
    score: 90,
    severity: null
  },
  {
    category: 'Compliance',
    description: 'Sample compliance description here',
    score: 100,
    severity: null
  }
];
