package org.gbif.predicate.query;

import com.google.common.collect.ImmutableSet;
import java.lang.annotation.Annotation;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.dwc.terms.*;
import org.gbif.occurrence.common.TermUtils;

public class SQLColumnsUtils {

  // reserved hive words
  public static final ImmutableSet<String> SQL_RESERVED_WORDS =
      new ImmutableSet.Builder<String>().add("date", "order", "format", "group").build();

  // prefix for extension columns
  private static final String EXTENSION_PRE = "ext_";

  private SQLColumnsUtils() {
    // empty constructor
  }

  public static String getHiveColumn(Term term) {
    if (GbifTerm.verbatimScientificName == term) {
      return "v_" + DwcTerm.scientificName.simpleName().toLowerCase();
    }
    String columnName = term.simpleName().toLowerCase();
    if (SQL_RESERVED_WORDS.contains(columnName)) {
      return columnName + '_';
    }
    return columnName;
  }

  /** Gets the Hive column name of the term parameter. */
  public static String getHiveQueryColumn(Term term) {
    String columnName = getHiveColumn(term);
    return isVocabulary(term) ? columnName + ".lineage" : columnName;
  }

  /** Gets the Hive column name of the term parameter. */
  public static String getHiveValueColumn(Term term) {
    String columnName = getHiveColumn(term);
    return isVocabulary(term) ? columnName + ".concept" : columnName;
  }

  /** Gets the Hive column name of the extension parameter. */
  public static String getHiveQueryColumn(Extension extension) {
    return EXTENSION_PRE + extension.name().toLowerCase();
  }

  /** Returns the Hive data type of term parameter. */
  public static String getHiveType(Term term) {
    if (TermUtils.isInterpretedNumerical(term)) {
      return "INT";
    } else if (TermUtils.isInterpretedLocalDate(term)) {
      return "BIGINT";
    } else if (TermUtils.isInterpretedUtcDate(term)) {
      return "BIGINT";
    } else if (TermUtils.isInterpretedDouble(term)) {
      return "DOUBLE";
    } else if (TermUtils.isInterpretedBoolean(term)) {
      return "BOOLEAN";
    } else if (isHiveArray(term)) {
      return "ARRAY<STRING>";
    } else if (TermUtils.isVocabulary(term)) {
      return "STRUCT<concept: STRING,lineage: ARRAY<STRING>>";
    } else {
      return "STRING";
    }
  }

  public static boolean isDate(Term term) {
    return TermUtils.isInterpretedLocalDate(term) || TermUtils.isInterpretedUtcDate(term);
  }

  /** Checks if the term is stored as an Hive array. */
  public static boolean isHiveArray(Term term) {
    return GbifTerm.mediaType == term
        || GbifTerm.issue == term
        || GbifInternalTerm.networkKey == term
        || DwcTerm.identifiedByID == term
        || DwcTerm.recordedByID == term
        || GbifInternalTerm.dwcaExtension == term
        || DwcTerm.datasetID == term
        || DwcTerm.datasetName == term
        || DwcTerm.typeStatus == term
        || DwcTerm.otherCatalogNumbers == term
        || DwcTerm.recordedBy == term
        || DwcTerm.identifiedBy == term
        || DwcTerm.preparations == term
        || DwcTerm.samplingProtocol == term;
  }

  /** Gets the Hive column name of the occurrence issue parameter. */
  public static String getHiveQueryColumn(OccurrenceIssue issue) {
    final String columnName = issue.name().toLowerCase();
    if (SQL_RESERVED_WORDS.contains(columnName)) {
      return columnName + '_';
    }
    return columnName;
  }

  public static boolean isVocabulary(Term term) {
    return term instanceof Enum && hasTermAnnotation(term, Vocabulary.class);
  }

  private static boolean hasTermAnnotation(Term term, Class<? extends Annotation> annotation) {
    try {
      return term.getClass().getField(((Enum) term).name()).isAnnotationPresent(annotation);
    } catch (NoSuchFieldException var3) {
      throw new IllegalArgumentException(var3);
    }
  }
}
