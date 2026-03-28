export class TestDataFactory {
  static createDatasetUrn(platform: string, name: string, env: string = 'PROD'): string {
    return `urn:li:dataset:(urn:li:dataPlatform:${platform},${name},${env})`;
  }

  static createUserUrn(username: string): string {
    return `urn:li:corpuser:${username}`;
  }

  static createBusinessAttributeUrn(name: string): string {
    return `urn:li:businessAttribute:${name}`;
  }

  static generateRandomString(length: number = 8): string {
    return Math.random().toString(36).substring(2, length + 2);
  }

  static generateTestDatasetName(): string {
    return `test_dataset_${this.generateRandomString()}`;
  }

  static generateTestUsername(): string {
    return `test_user_${this.generateRandomString()}`;
  }
}
