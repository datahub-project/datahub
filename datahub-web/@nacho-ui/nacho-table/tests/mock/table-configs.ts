import { INachoTableConfigs } from '@nacho-ui/table/types/nacho-table';

interface IMockOptions {
  withCustomRow: boolean;
}

export default function getMockTableConfigs(options: IMockOptions = {} as IMockOptions): INachoTableConfigs<unknown> {
  const baseMockConfigs: INachoTableConfigs<unknown> = {
    labels: ['name', 'type', 'nature'],
    headers: [{ title: 'Name' }, { title: 'Type' }, { title: 'Nature' }]
  };

  if (options.withCustomRow) {
    baseMockConfigs.customRows = {
      component: 'config-cases/helpers/test-row'
    };

    baseMockConfigs.labels.push('trainer');
    baseMockConfigs.headers?.push({ title: 'Trainer' });
  }

  return baseMockConfigs;
}
