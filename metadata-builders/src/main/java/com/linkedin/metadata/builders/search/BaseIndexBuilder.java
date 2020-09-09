package com.linkedin.metadata.builders.search;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.validator.DocumentValidator;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Base class for populating documents from a metadata snapshot
 *
 * @param <DOCUMENT> the type of document that will be populated and returned
 */
public abstract class BaseIndexBuilder<DOCUMENT extends RecordTemplate> {

  final List<Class<? extends RecordTemplate>> _snapshotsInterested;

  /**
   * Constructor
   *
   * @param snapshotsInterested List of metadata snapshot classes the document index builder is interested in
   * @param documentClass class of DOCUMENT that should have a valid schema
   */
  BaseIndexBuilder(@Nonnull List<Class<? extends RecordTemplate>> snapshotsInterested, @Nonnull Class<DOCUMENT> documentClass) {
    DocumentValidator.validateDocumentSchema(documentClass);
    _snapshotsInterested = Collections.unmodifiableList(snapshotsInterested);
  }

  /**
   * Constructs documents to update from a metadata snapshot
   *
   * <p> Given a metadata snapshot containing a list of metadata aspects, this function returns list of documents.
   *
   * <p> Each document is obtained from parsing a metadata aspect from the metadata snapshot that is relevant to
   * the document index builder that inherits this class.
   *
   * Each document index builder that inherits from this class, should subscribe to the metadata snapshots it is interested
   * in by calling the constructor of this class with the list of metadata snapshot classes
   *
   * @param snapshot Metadata snapshot from which document has to be parsed
   * @return list of documents obtained from various aspects inside a metadata snapshot
   */
  @Nullable
  public abstract List<DOCUMENT> getDocumentsToUpdate(@Nonnull RecordTemplate snapshot);

  @Nonnull
  public abstract Class<DOCUMENT> getDocumentType();
}
