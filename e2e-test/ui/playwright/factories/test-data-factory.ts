import { withTimestamp } from '../utils/random';

/** Alias for clarity when a string represents a DataHub URN. */
export type Urn = string;

export class TestDataFactory {
  static createDatasetUrn(platform: string, name: string, env: string = 'PROD'): Urn {
    return `urn:li:dataset:(urn:li:dataPlatform:${platform},${name},${env})`;
  }

  static createUserUrn(username: string): Urn {
    return `urn:li:corpuser:${username}`;
  }

  static createBusinessAttributeUrn(name: string): Urn {
    return `urn:li:businessAttribute:${name}`;
  }

  /**
   * Append a timestamp suffix so test data names are unique and sortable by
   * creation time. Format: YYYYMMDD_HHmmss.
   */
  static withTimestamp(base: string): string {
    return withTimestamp(base);
  }

  static generateTestDatasetName(): string {
    return withTimestamp('test_dataset');
  }

  static generateTestUsername(): string {
    return withTimestamp('test_user');
  }

  static generateTestAttributeName(): string {
    return withTimestamp('test_attribute');
  }
}
