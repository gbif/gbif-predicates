package org.gbif.predicate.query;

import com.google.common.collect.ImmutableSet;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.*;

public class SQLColumnsUtils {

  // reserved hive words
  public static final ImmutableSet<String> SQL_RESERVED_WORDS =
      new ImmutableSet.Builder<String>().add("date", "order", "format", "group").build();

  // prefix for extension columns
  private static final String EXTENSION_PRE = "ext_";

  private static final Set<? extends Term> EXTENSION_TERMS =
      Arrays.stream(Extension.values())
          .map(ext -> TermFactory.instance().findTerm(ext.getRowType()))
          .collect(Collectors.toSet());

  private static final Set<? extends Term> INTERPRETED_LOCAL_DATES =
      ImmutableSet.of(
          DwcTerm.dateIdentified, GbifInternalTerm.eventDateGte, GbifInternalTerm.eventDateLte);

  private static final Set<? extends Term> INTERPRETED_UTC_DATES =
      ImmutableSet.of(
          GbifTerm.lastInterpreted,
          GbifTerm.lastParsed,
          GbifTerm.lastCrawled,
          DcTerm.modified,
          GbifInternalTerm.fragmentCreated);

  private static final Set<? extends Term> INTERPRETED_NUM =
      ImmutableSet.of(
          DwcTerm.year,
          DwcTerm.month,
          DwcTerm.day,
          DwcTerm.individualCount,
          GbifTerm.taxonKey,
          GbifTerm.kingdomKey,
          GbifTerm.phylumKey,
          GbifTerm.classKey,
          GbifTerm.orderKey,
          GbifTerm.familyKey,
          GbifTerm.genusKey,
          GbifTerm.subgenusKey,
          GbifTerm.speciesKey,
          GbifTerm.acceptedTaxonKey,
          GbifInternalTerm.crawlId,
          GbifInternalTerm.identifierCount);

  private static final Set<? extends Term> INTERPRETED_BOOLEAN =
      ImmutableSet.of(GbifTerm.hasCoordinate, GbifTerm.hasGeospatialIssues);

  private static final Set<? extends Term> COMPLEX_TYPE =
      ImmutableSet.of(
          GbifTerm.mediaType,
          GbifTerm.issue,
          GbifInternalTerm.networkKey,
          DwcTerm.identifiedByID,
          DwcTerm.recordedByID,
          GbifInternalTerm.dwcaExtension,
          DwcTerm.datasetName,
          DwcTerm.datasetID,
          DwcTerm.typeStatus,
          DwcTerm.otherCatalogNumbers,
          DwcTerm.recordedBy,
          DwcTerm.identifiedBy,
          DwcTerm.georeferencedBy,
          DwcTerm.higherGeography,
          DwcTerm.preparations,
          DwcTerm.samplingProtocol);

  private static final Set<? extends Term> INTERPRETED_DOUBLE =
      ImmutableSet.of(
          DwcTerm.decimalLatitude,
          DwcTerm.decimalLongitude,
          GbifTerm.coordinateAccuracy,
          GbifTerm.elevation,
          GbifTerm.elevationAccuracy,
          GbifTerm.depth,
          GbifTerm.depthAccuracy,
          GbifTerm.distanceFromCentroidInMeters,
          DwcTerm.coordinateUncertaintyInMeters,
          DwcTerm.coordinatePrecision);

  private SQLColumnsUtils() {
    // empty constructor
  }

  public static String getSQLColumn(Term term) {
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
  public static String getSQLQueryColumn(Term term) {
    String columnName = getSQLColumn(term);
    return isVocabulary(term) ? columnName + ".lineage" : columnName;
  }

  /** Gets the Hive column name of the term parameter. */
  public static String getSQLValueColumn(Term term) {
    String columnName = getSQLColumn(term);
    return isVocabulary(term) ? columnName + ".concept" : columnName;
  }

  /** Gets the Hive column name of the extension parameter. */
  public static String getSQLQueryColumn(Extension extension) {
    return EXTENSION_PRE + extension.name().toLowerCase();
  }

  /** Returns the Hive data type of term parameter. */
  public static String getSQLType(Term term) {
    if (isInterpretedNumerical(term)) {
      return "INT";
    } else if (isInterpretedLocalDate(term)) {
      return "BIGINT";
    } else if (isInterpretedUtcDate(term)) {
      return "BIGINT";
    } else if (isInterpretedDouble(term)) {
      return "DOUBLE";
    } else if (isInterpretedBoolean(term)) {
      return "BOOLEAN";
    } else if (isSQLArray(term)) {
      return "ARRAY<STRING>";
    } else if (isVocabulary(term)) {
      return "STRUCT<concept: STRING,lineage: ARRAY<STRING>>";
    } else {
      return "STRING";
    }
  }

  public static boolean isDate(Term term) {
    return isInterpretedLocalDate(term) || isInterpretedUtcDate(term);
  }

  /** Checks if the term is stored as an Hive array. */
  public static boolean isSQLArray(Term term) {
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
        || DwcTerm.samplingProtocol == term
        || DwcTerm.georeferencedBy == term
        || DwcTerm.higherGeography == term;
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

  /** @return true if the term is an interpreted local date (timezone not relevant) */
  public static boolean isInterpretedLocalDate(Term term) {
    return INTERPRETED_LOCAL_DATES.contains(term);
  }

  /** @return true if the term is an interpreted UTC date with */
  public static boolean isInterpretedUtcDate(Term term) {
    return INTERPRETED_UTC_DATES.contains(term);
  }

  /** @return true if the term is an interpreted numerical */
  public static boolean isInterpretedNumerical(Term term) {
    return INTERPRETED_NUM.contains(term);
  }

  /** @return true if the term is an interpreted double */
  public static boolean isInterpretedDouble(Term term) {
    return INTERPRETED_DOUBLE.contains(term);
  }

  /** @return true if the term is an interpreted boolean */
  public static boolean isInterpretedBoolean(Term term) {
    return INTERPRETED_BOOLEAN.contains(term);
  }

  /** @return true if the term is a complex type in Hive: array, struct, json, etc. */
  public static boolean isComplexType(Term term) {
    return COMPLEX_TYPE.contains(term);
  }

  public static boolean isExtensionTerm(Term term) {
    return EXTENSION_TERMS.contains(term);
  }
}
