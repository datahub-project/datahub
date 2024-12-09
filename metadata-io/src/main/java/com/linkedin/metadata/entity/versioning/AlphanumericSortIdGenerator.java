package com.linkedin.metadata.entity.versioning;

public class AlphanumericSortIdGenerator {

  private AlphanumericSortIdGenerator() {}

  private static final int STRING_LENGTH = 8;
  private static final char[] ALLOWED_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

  /**
   * Increments an 8-character alphanumeric string. For example: "AAAAAAAA" -> "AAAAAAAB" "AAAAAAAZ"
   * -> "AAAAAABA"
   *
   * @param currentId The current 8-character string
   * @return The next string in sequence
   * @throws IllegalArgumentException if input string is not 8 characters or contains invalid
   *     characters
   */
  public static String increment(String currentId) {
    if (currentId == null || currentId.length() != STRING_LENGTH) {
      throw new IllegalArgumentException("Input string must be exactly 8 characters long");
    }

    // Convert string to char array for manipulation
    char[] currentIdChars = currentId.toCharArray();

    // Validate input characters
    for (char c : currentIdChars) {
      if (getCharIndex(c) == -1) {
        throw new IllegalArgumentException("Invalid character in input string: " + c);
      }
    }

    // Start from rightmost position
    for (int i = STRING_LENGTH - 1; i >= 0; i--) {
      int currentCharIndex = getCharIndex(currentIdChars[i]);

      // If current character is not the last allowed character,
      // simply increment it and we're done
      if (currentCharIndex < ALLOWED_CHARS.length - 1) {
        currentIdChars[i] = ALLOWED_CHARS[currentCharIndex + 1];
        return new String(currentIdChars);
      }

      // If we're here, we need to carry over to next position
      currentIdChars[i] = ALLOWED_CHARS[0];

      // If we're at the leftmost position and need to carry,
      // we've reached maximum value and need to wrap around
      if (i == 0) {
        return "AAAAAAAA";
      }
    }

    // Should never reach here
    throw new RuntimeException("Unexpected error in increment operation");
  }

  /**
   * Gets the index of a character in the ALLOWED_CHARS array. Returns -1 if character is not found.
   */
  private static int getCharIndex(char c) {
    for (int i = 0; i < ALLOWED_CHARS.length; i++) {
      if (ALLOWED_CHARS[i] == c) {
        return i;
      }
    }
    return -1;
  }
}
