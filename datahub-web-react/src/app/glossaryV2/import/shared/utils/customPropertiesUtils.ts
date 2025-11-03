/**
 * Utility functions for handling custom properties in different formats
 */

/**
 * Parse custom properties from various formats
 * Supports both JSON format (recommended for CSV) and string format (from GraphQL)
 */
export function parseCustomProperties(input: string | null | undefined): Record<string, string> {
  if (!input || input.trim() === '') {
    return {};
  }

  const trimmed = input.trim();

  if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
    try {
      return JSON.parse(trimmed);
    } catch (error) {
      console.warn('Failed to parse custom properties as JSON:', error);
    }
  }

  const result: Record<string, string> = {};
  const pairs = trimmed.split(',');
  
  pairs.forEach(pair => {
    const colonIndex = pair.indexOf(':');
    if (colonIndex > 0) {
      const key = pair.substring(0, colonIndex).trim();
      const value = pair.substring(colonIndex + 1).trim();
      if (key && value) {
        result[key] = value;
      }
    }
  });

  return result;
}

export function formatCustomPropertiesForCsv(properties: Record<string, string>): string {
  if (!properties || Object.keys(properties).length === 0) {
    return '';
  }
  return JSON.stringify(properties);
}

export function formatCustomPropertiesForGraphQL(properties: Record<string, string>): string {
  if (!properties || Object.keys(properties).length === 0) {
    return '';
  }
  return Object.entries(properties)
    .map(([key, value]) => `${key}:${value}`)
    .join(',');
}

export function normalizeCustomProperties(input: string | null | undefined): Record<string, string> {
  return parseCustomProperties(input);
}

export function compareCustomProperties(prop1: string | null | undefined, prop2: string | null | undefined): boolean {
  const obj1 = normalizeCustomProperties(prop1 || '');
  const obj2 = normalizeCustomProperties(prop2 || '');
  
  const keys1 = Object.keys(obj1).sort();
  const keys2 = Object.keys(obj2).sort();
  
  if (keys1.length !== keys2.length) {
    return false;
  }
  
  return keys1.every(key => obj1[key] === obj2[key]);
}
