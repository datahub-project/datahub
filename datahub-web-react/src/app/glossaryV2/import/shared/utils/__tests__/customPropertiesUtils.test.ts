import { describe, it, expect } from 'vitest';
import { compareCustomProperties } from '../customPropertiesUtils';

describe('Custom Properties Utils', () => {
  describe('compareCustomProperties', () => {
    it('should compare identical custom properties correctly', () => {
      const props1 = '{"data_type":"Dataset","domain":"Test Domain"}';
      const props2 = '{"data_type":"Dataset","domain":"Test Domain"}';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(true);
    });

    it('should compare different custom properties correctly', () => {
      const props1 = '{"data_type":"Dataset","domain":"Test Domain"}';
      const props2 = '{"data_type":"Table","domain":"Test Domain"}';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(false);
    });

    it('should handle empty custom properties', () => {
      const props1 = '';
      const props2 = '';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(true);
    });

    it('should handle null custom properties', () => {
      const props1 = null;
      const props2 = null;

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(true);
    });

    it('should handle undefined custom properties', () => {
      const props1 = undefined;
      const props2 = undefined;

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(true);
    });

    it('should handle mixed null and empty custom properties', () => {
      const props1 = null;
      const props2 = '';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(true);
    });

    it('should handle one null and one with value', () => {
      const props1 = null;
      const props2 = '{"data_type":"Dataset"}';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(false);
    });

    it('should handle one empty and one with value', () => {
      const props1 = '';
      const props2 = '{"data_type":"Dataset"}';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(false);
    });

    it('should handle malformed JSON gracefully', () => {
      const props1 = '{"data_type":"Dataset"';
      const props2 = '{"data_type":"Dataset"}';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(false);
    });

    it('should handle different property orders', () => {
      const props1 = '{"data_type":"Dataset","domain":"Test Domain"}';
      const props2 = '{"domain":"Test Domain","data_type":"Dataset"}';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(true);
    });

    it('should handle different whitespace', () => {
      const props1 = '{"data_type":"Dataset","domain":"Test Domain"}';
      const props2 = '{"data_type": "Dataset", "domain": "Test Domain"}';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(true);
    });

    it('should handle nested objects as string values', () => {
      const props1 = '{"metadata":"{\\"type\\":\\"Dataset\\",\\"version\\":\\"1.0\\"}"}';
      const props2 = '{"metadata":"{\\"type\\":\\"Dataset\\",\\"version\\":\\"1.0\\"}"}';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(true);
    });

    it('should handle arrays as string values', () => {
      const props1 = '{"tags":"tag1,tag2,tag3"}';
      const props2 = '{"tags":"tag1,tag2,tag3"}';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(true);
    });

    it('should handle different array orders as string values', () => {
      const props1 = '{"tags":"tag1,tag2,tag3"}';
      const props2 = '{"tags":"tag3,tag1,tag2"}';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(false);
    });

    it('should handle boolean values', () => {
      const props1 = '{"is_active":true,"is_public":false}';
      const props2 = '{"is_active":true,"is_public":false}';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(true);
    });

    it('should handle numeric values', () => {
      const props1 = '{"count":42,"rate":3.14}';
      const props2 = '{"count":42,"rate":3.14}';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(true);
    });

    it('should handle mixed data types as string values', () => {
      const props1 = '{"name":"test","count":"42","is_active":"true","tags":"tag1,tag2"}';
      const props2 = '{"name":"test","count":"42","is_active":"true","tags":"tag1,tag2"}';

      const result = compareCustomProperties(props1, props2);
      expect(result).toBe(true);
    });
  });
});
