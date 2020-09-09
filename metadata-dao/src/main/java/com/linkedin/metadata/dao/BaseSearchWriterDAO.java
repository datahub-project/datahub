package com.linkedin.metadata.dao;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.validator.DocumentValidator;
import javax.annotation.Nonnull;


/**
 * A base class for all Search Writer DAOs.
 *
 * Search Writer DAO is a standardized interface to update a search index.
 */
public abstract class BaseSearchWriterDAO<DOCUMENT extends RecordTemplate> {

  protected final Class<DOCUMENT> _documentClass;

  public BaseSearchWriterDAO(@Nonnull Class<DOCUMENT> documentClass) {
    DocumentValidator.validateDocumentSchema(documentClass);
    _documentClass = documentClass;
  }

  /**
   * Updates or inserts the given search document.
   *
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public abstract void upsertDocument(@Nonnull DOCUMENT document, @Nonnull String docId);

  /**
   * Deletes the document with the given document ID from the index.
   */
  public abstract void deleteDocument(@Nonnull String docId);

  /**
   * Closes this writer, releasing any associated resources.
   */
  public abstract void close();
}
