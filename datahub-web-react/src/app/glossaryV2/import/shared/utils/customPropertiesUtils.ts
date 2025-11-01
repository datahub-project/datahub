/**
 * Utility functions for handling custom properties in different formats
 */

/**
 * Parse custom properties from various formats
 * Supports both JSON format (recommended for CSV) and string format (from GraphQL)
 */
export function parseCustomProperties(input: string): Record<string, string> {
  if (!input || input.trim() === '') {
    return {};
  }

  const trimmed = input.trim();

  // Try JSON format first (recommended for CSV)
  if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
    try {
      return JSON.parse(trimmed);
    } catch (error) {
      console.warn('Failed to parse custom properties as JSON:', error);
      // Fall back to string format parsing
    }
  }

  // Parse string format (from GraphQL): "key1:value1,key2:value2"
  const result: Record<string, string> = {};
  const pairs = trimmed.split(',');
  
  for (const pair of pairs) {
    const colonIndex = pair.indexOf(':');
    if (colonIndex > 0) {
      const key = pair.substring(0, colonIndex).trim();
      const value = pair.substring(colonIndex + 1).trim();
      if (key && value) {
        result[key] = value;
      }
    }
  }

  return result;
}

/**
 * Convert custom properties to JSON format (recommended for CSV)
 */
export function formatCustomPropertiesForCsv(properties: Record<string, string>): string {
  if (!properties || Object.keys(properties).length === 0) {
    return '';
  }
  return JSON.stringify(properties);
}

/**
 * Convert custom properties to string format (for GraphQL compatibility)
 */
export function formatCustomPropertiesForGraphQL(properties: Record<string, string>): string {
  if (!properties || Object.keys(properties).length === 0) {
    return '';
  }
  return Object.entries(properties)
    .map(([key, value]) => `${key}:${value}`)
    .join(',');
}

/**
 * Normalize custom properties for comparison
 * Converts both formats to a consistent object format
 */
export function normalizeCustomProperties(input: string): Record<string, string> {
  return parseCustomProperties(input);
}

/**
 * Check if two custom properties objects are equal
 */
export function compareCustomProperties(prop1: string, prop2: string): boolean {
  const obj1 = normalizeCustomProperties(prop1);
  const obj2 = normalizeCustomProperties(prop2);
  
  const keys1 = Object.keys(obj1).sort();
  const keys2 = Object.keys(obj2).sort();
  
  if (keys1.length !== keys2.length) {
    return false;
  }
  
  return keys1.every(key => obj1[key] === obj2[key]);
}
