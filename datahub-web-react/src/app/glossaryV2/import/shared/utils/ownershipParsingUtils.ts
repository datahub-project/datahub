/**
 * Utility functions for parsing ownership information from CSV strings
 */

export interface ParsedOwnership {
  ownershipTypeName: string;
  ownerName: string;
  corpType: string;
  ownerUrn: string;
  ownerType: 'TECHNICAL_OWNER' | 'BUSINESS_OWNER' | 'DATA_STEWARD' | 'NONE';
}

/**
 * Parses ownership from separate user and group columns
 * @param usersColumn - Column containing user ownership in format "user:ownershipType|user2:ownershipType2"
 * @param groupsColumn - Column containing group ownership in format "group:ownershipType|group2:ownershipType2"
 * @returns Array of ParsedOwnership objects
 */
export function parseOwnershipFromColumns(usersColumn: string, groupsColumn: string): ParsedOwnership[] {
  const results: ParsedOwnership[] = [];
  
  // Parse users column
  if (usersColumn && usersColumn.trim()) {
    const userEntries = usersColumn.split('|').map(entry => entry.trim()).filter(entry => entry);
    for (const entry of userEntries) {
      const parsed = parseSingleOwnership(entry, 'CORP_USER');
      if (parsed) {
        results.push(parsed);
      }
    }
  }
  
  // Parse groups column
  if (groupsColumn && groupsColumn.trim()) {
    const groupEntries = groupsColumn.split('|').map(entry => entry.trim()).filter(entry => entry);
    for (const entry of groupEntries) {
      const parsed = parseSingleOwnership(entry, 'CORP_GROUP');
      if (parsed) {
        results.push(parsed);
      }
    }
  }
  
  return results;
}

/**
 * Parses a single ownership string in the format "owner:ownershipType"
 * @param ownershipString - The ownership string to parse
 * @param corpType - The corp type (CORP_USER or CORP_GROUP)
 * @returns ParsedOwnership object or null if invalid
 */
export function parseSingleOwnership(ownershipString: string, corpType: 'CORP_USER' | 'CORP_GROUP' = 'CORP_USER'): ParsedOwnership | null {
  const trimmed = ownershipString.trim();
  if (!trimmed) return null;

  const parts = trimmed.split(':');
  if (parts.length < 2) return null;

  const [ownerName, ownershipTypeName] = parts.map(part => part.trim());
  
  // Handle case where ownerName contains colons (like URNs)
  if (parts.length > 2) {
    // For URNs, the last part is the ownership type, everything else is the owner URN
    const ownershipTypeName = parts[parts.length - 1];
    const ownerName = parts.slice(0, -1).join(':');
    return {
      ownershipTypeName,
      ownerName,
      corpType: 'CORP_USER', // Default for URNs
      ownerUrn: ownerName,
      ownerType: 'NONE' as const
    };
  }
  
  if (!ownerName || !ownershipTypeName) return null;

  // Generate owner URN based on corpType
  let ownerUrn: string;
  if (ownerName.startsWith('urn:li:')) {
    ownerUrn = ownerName;
  } else if (corpType === 'CORP_GROUP') {
    ownerUrn = `urn:li:corpGroup:${ownerName}`;
  } else {
    ownerUrn = `urn:li:corpuser:${ownerName}`;
  }
  
  // Determine owner type based on ownership type name
  let ownerType: 'TECHNICAL_OWNER' | 'BUSINESS_OWNER' | 'DATA_STEWARD' | 'NONE' = 'NONE';
  const typeName = ownershipTypeName.toLowerCase();
  if (typeName.includes('technical')) {
    ownerType = 'TECHNICAL_OWNER';
  } else if (typeName.includes('business')) {
    ownerType = 'BUSINESS_OWNER';
  } else if (typeName.includes('data') && !typeName.includes('datahub')) {
    ownerType = 'DATA_STEWARD';
  }

  return {
    ownershipTypeName,
    ownerName,
    corpType,
    ownerUrn,
    ownerType
  };
}

/**
 * Parses multiple ownership strings separated by comma or pipe
 * @param ownershipString - The ownership string containing multiple entries
 * @returns Array of ParsedOwnership objects
 */
export function parseOwnershipString(ownershipString: string): ParsedOwnership[] {
  if (!ownershipString) return [];

  // Support both comma and pipe separators
  const ownershipStrings = ownershipString
    .split(/[|,]/)
    .map(owner => owner.trim())
    .filter(owner => owner.length > 0);

  return ownershipStrings
    .map(ownershipString => parseSingleOwnership(ownershipString, 'CORP_USER'))
    .filter((parsed): parsed is ParsedOwnership => parsed !== null);
}

/**
 * Validates ownership string format
 * @param ownershipString - The ownership string to validate
 * @returns Object with validation result and error message
 */
export function validateOwnershipString(ownershipString: string): { isValid: boolean; error?: string } {
  if (!ownershipString) {
    return { isValid: true }; // Empty ownership is valid
  }

  const parsed = parseOwnershipString(ownershipString);
  if (parsed.length === 0) {
    return { 
      isValid: false, 
      error: `Invalid ownership format: "${ownershipString}". Expected format: "owner:ownershipType:corpType"` 
    };
  }

  // Check for empty entries (like "admin:DEVELOPER:CORP_USER,,datahub:Technical Owner:CORP_USER")
  const hasEmptyEntries = ownershipString.split(/[|,]/).some(entry => entry.trim() === '');
  if (hasEmptyEntries) {
    return { 
      isValid: false, 
      error: `Invalid ownership format: "${ownershipString}". Empty ownership entries are not allowed.` 
    };
  }

  return { isValid: true };
}

/**
 * Validates ownership columns format
 * @param usersColumn - Column containing user ownership
 * @param groupsColumn - Column containing group ownership
 * @returns Validation result with isValid flag and optional error message
 */
export function validateOwnershipColumns(usersColumn: string, groupsColumn: string): { isValid: boolean; error?: string } {
  if (!usersColumn && !groupsColumn) {
    return { isValid: true }; // Empty ownership is valid
  }

  // Validate users column
  if (usersColumn && usersColumn.trim()) {
    const userEntries = usersColumn.split('|').map(entry => entry.trim());
    for (const entry of userEntries) {
      if (!entry) {
        return { 
          isValid: false, 
          error: 'Empty ownership entries are not allowed' 
        };
      }
      const parts = entry.split(':');
      if (parts.length < 2) {
        return { 
          isValid: false, 
          error: `Invalid user ownership format: "${entry}". Expected format: "user:ownershipType"` 
        };
      }
    }
  }

  // Validate groups column
  if (groupsColumn && groupsColumn.trim()) {
    const groupEntries = groupsColumn.split('|').map(entry => entry.trim());
    for (const entry of groupEntries) {
      if (!entry) {
        return { 
          isValid: false, 
          error: 'Empty ownership entries are not allowed' 
        };
      }
      const parts = entry.split(':');
      if (parts.length < 2) {
        return { 
          isValid: false, 
          error: `Invalid group ownership format: "${entry}". Expected format: "group:ownershipType"` 
        };
      }
    }
  }

  return { isValid: true };
}

/**
 * Creates ownership patch operations for DataHub
 * @param parsedOwnership - Array of parsed ownership objects
 * @param ownershipTypeMap - Map of ownership type names to URNs
 * @param isUpdate - Whether this is updating an existing entity (uses REPLACE) or creating new (uses ADD)
 * @returns Array of patch operations
 */
export function createOwnershipPatchOperations(
  parsedOwnership: ParsedOwnership[],
  ownershipTypeMap: Map<string, string>,
  isUpdate: boolean = false
): Array<{ op: 'ADD' | 'REMOVE' | 'REPLACE' | 'MOVE' | 'COPY' | 'TEST'; path: string; value: string }> {
  const patches: Array<{ op: 'ADD' | 'REMOVE' | 'REPLACE' | 'MOVE' | 'COPY' | 'TEST'; path: string; value: string }> = [];

  if (isUpdate) {
    // For existing entities, replace the entire owners array to avoid stale data
    const owners = parsedOwnership.map(({ ownershipTypeName, ownerUrn, ownerType }) => {
      const ownershipTypeUrn = ownershipTypeMap.get(ownershipTypeName.toLowerCase());
      if (!ownershipTypeUrn) {
        throw new Error(`Ownership type "${ownershipTypeName}" not found in ownership type map`);
      }

      return {
        owner: ownerUrn,
        typeUrn: ownershipTypeUrn,
        type: ownerType,
        source: { type: 'MANUAL' }
      };
    });

    // Group owners by ownership type for the ownerTypes structure
    const ownerTypes: Record<string, string[]> = {};
    owners.forEach(owner => {
      if (!ownerTypes[owner.typeUrn]) {
        ownerTypes[owner.typeUrn] = [];
      }
      ownerTypes[owner.typeUrn].push(owner.owner);
    });

    // Replace the owners array directly (no /value wrapper)
    patches.push({
      op: 'REPLACE' as const,
      path: '/owners',
      value: JSON.stringify(owners)
    });

    // Replace the ownerTypes map
    patches.push({
      op: 'REPLACE' as const,
      path: '/ownerTypes',
      value: JSON.stringify(ownerTypes)
    });
  } else {
    // For new entities, add individual owners
    parsedOwnership.forEach(({ ownershipTypeName, ownerUrn, ownerType }) => {
      const ownershipTypeUrn = ownershipTypeMap.get(ownershipTypeName.toLowerCase());
      if (!ownershipTypeUrn) {
        throw new Error(`Ownership type "${ownershipTypeName}" not found in ownership type map`);
      }

      // Use the ownership type string in the path, not the URN
      // The path format should be: /owners/{ownerUrn}/{ownershipType}
      patches.push({
        op: 'ADD' as const,
        path: `/owners/${ownerUrn}/${ownerType}`,
        value: JSON.stringify({
          owner: ownerUrn,
          typeUrn: ownershipTypeUrn,
          type: ownerType,
          source: { type: 'MANUAL' }
        })
      });
    });
  }

  return patches;
}
