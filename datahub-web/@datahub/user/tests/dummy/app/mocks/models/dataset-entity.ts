import { FakeDatasetEntity as mockEntity } from '@datahub/user/mocks/models/dataset-entity';

/**
 * Temporarily importing the mock function from the addon/ folder. However, that's only there for
 * demo purposes. After implementing real data in the integrated application, the function should
 * be moved here where it will only be used for testing purposes
 */
export const FakeDatasetEntity = mockEntity;
