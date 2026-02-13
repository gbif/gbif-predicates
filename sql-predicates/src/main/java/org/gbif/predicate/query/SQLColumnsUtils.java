package org.gbif.predicate.query;

import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.*;

// TODO: humboldt
public class SQLColumnsUtils {

  // reserved hive words
  public static final Set<String> SQL_RESERVED_WORDS = Set.of("date", "order", "format", "group");

  // prefix for extension columns
  private static final String EXTENSION_PRE = "ext_";

  private static final Set<? extends Term> EXTENSION_TERMS =
      Arrays.stream(Extension.values())
          .map(ext -> TermFactory.instance().findTerm(ext.getRowType()))
          .collect(Collectors.toSet());

  private static final Set<? extends Term> INTERPRETED_LOCAL_DATES_SECONDS =
      Set.of(DwcTerm.dateIdentified, GbifInternalTerm.eventDateGte, GbifInternalTerm.eventDateLte);

  private static final Set<? extends Term> INTERPRETED_UTC_DATES_SECONDS = Set.of(DcTerm.modified);

  private static final Set<? extends Term> INTERPRETED_UTC_DATES_MILLISECONDS =
      Set.of(
          GbifTerm.lastInterpreted,
          GbifTerm.lastParsed,
          GbifTerm.lastCrawled,
          GbifInternalTerm.fragmentCreated);

  private static final Set<? extends Term> INTERPRETED_NUM =
      Set.of(
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
      Set.of(GbifTerm.hasCoordinate, GbifTerm.hasGeospatialIssues);

  private static final Set<? extends Term> COMPLEX_TYPE =
      Set.of(
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
      Set.of(
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
    if (isHumboldtTerm(term)) {
      columnName = "h." + columnName;
    }
    return isVocabulary(term) ? columnName + ".lineage" : columnName;
  }

  /** Gets the Hive column name of the term parameter. */
  public static String getSQLValueColumn(Term term) {
    String columnName = getSQLColumn(term);
    return isVocabulary(term)
        ? isSQLArray(term) ? columnName + ".concepts" : columnName + ".concept"
        : columnName;
  }

  /** Gets the Hive column name of the extension parameter. */
  public static String getSQLQueryColumn(Extension extension) {
    return EXTENSION_PRE + extension.name().toLowerCase();
  }

  /** Returns the Hive data type of term parameter. */
  public static String getSQLType(Term term) {
    if (isInterpretedNumerical(term)) {
      return "INT";
    } else if (isInterpretedLocalDateSeconds(term)) {
      return "BIGINT";
    } else if (isInterpretedUtcDateSeconds(term)) {
      return "BIGINT";
    } else if (isInterpretedUtcDateMilliseconds(term)) {
      return "BIGINT";
    } else if (isInterpretedDouble(term)) {
      return "DOUBLE";
    } else if (isInterpretedBoolean(term)) {
      return "BOOLEAN";
    } else if (isVocabulary(term)) {
      if (isSQLArray(term)) {
        return "STRUCT<concepts: ARRAY<STRING>,lineage: ARRAY<STRING>>";
      } else {
        return "STRUCT<concept: STRING,lineage: ARRAY<STRING>>";
      }
    } else if (isSQLArray(term)) {
      return "ARRAY<STRING>";
    } else {
      return "STRING";
    }
  }

  public static boolean isDate(Term term) {
    return isInterpretedLocalDateSeconds(term)
        || isInterpretedUtcDateSeconds(term)
        || isInterpretedUtcDateMilliseconds(term);
  }

  /** Checks if the term is stored as a Hive array. */
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
        || DwcTerm.higherGeography == term
        || GbifTerm.projectId == term
        || GbifTerm.lithostratigraphy == term
        || GbifTerm.biostratigraphy == term
        || GbifTerm.nonTaxonomicIssue == term
        || EcoTerm.voucherInstitutions == term
        || EcoTerm.verbatimSiteNames == term
        || EcoTerm.verbatimSiteDescriptions == term
        || EcoTerm.compilationSourceTypes == term
        || EcoTerm.targetHabitatScope == term
        || EcoTerm.excludedHabitatScope == term
        || EcoTerm.targetGrowthFormScope == term
        || EcoTerm.excludedGrowthFormScope == term
        || EcoTerm.taxonCompletenessProtocols == term
        || EcoTerm.samplingPerformedBy == term
        || EcoTerm.compilationTypes == term
        || EcoTerm.materialSampleTypes == term
        || EcoTerm.inventoryTypes == term
        || EcoTerm.protocolNames == term
        || EcoTerm.protocolDescriptions == term
        || EcoTerm.protocolReferences == term
        || EcoTerm.targetLifeStageScope == term
        || EcoTerm.targetDegreeOfEstablishmentScope == term
        || EcoTerm.excludedLifeStageScope == term
        || EcoTerm.excludedDegreeOfEstablishmentScope == term
        || DwcTerm.projectTitle == term
        || DwcTerm.fundingAttribution == term
        || DwcTerm.fundingAttributionID == term
        || DwcTerm.measurementType == term
        || ObisTerm.measurementTypeID == term;
  }

  public static boolean isHumboldtTerm(Term term) {
    return term instanceof EcoTerm || term == GbifInternalTerm.humboldtEventDurationValueInMinutes;
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
  public static boolean isInterpretedLocalDateSeconds(Term term) {
    return term != null && INTERPRETED_LOCAL_DATES_SECONDS.contains(term);
  }

  /** @return true if the term is an interpreted UTC date stored in seconds */
  public static boolean isInterpretedUtcDateSeconds(Term term) {
    return term != null && INTERPRETED_UTC_DATES_SECONDS.contains(term);
  }

  /** @return true if the term is an interpreted UTC date stored in milliseconds */
  public static boolean isInterpretedUtcDateMilliseconds(Term term) {
    return term != null && INTERPRETED_UTC_DATES_MILLISECONDS.contains(term);
  }

  /** @return true if the term is an interpreted numerical */
  public static boolean isInterpretedNumerical(Term term) {
    return term != null && INTERPRETED_NUM.contains(term);
  }

  /** @return true if the term is an interpreted double */
  public static boolean isInterpretedDouble(Term term) {
    return term != null && INTERPRETED_DOUBLE.contains(term);
  }

  /** @return true if the term is an interpreted boolean */
  public static boolean isInterpretedBoolean(Term term) {
    return term != null && INTERPRETED_BOOLEAN.contains(term);
  }

  /** @return true if the term is a complex type in Hive: array, struct, json, etc. */
  public static boolean isComplexType(Term term) {
    return term != null && COMPLEX_TYPE.contains(term);
  }

  public static boolean isExtensionTerm(Term term) {
    return term != null && EXTENSION_TERMS.contains(term);
  }

  public static Map<SearchParameter, String> HUMBOLDT_TAXON_COLUMNS =
      Map.of(
          EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY,
          "usagekey",
          EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_NAME,
          "usagename",
          EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_ACCEPTED_USAGE_KEY,
          "acceptedusagekey",
          EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_ACCEPTED_USAGE_NAME,
          "acceptedusagename",
          EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_TAXON_KEY,
          "taxonkeys",
          EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_IUCN_RED_LIST_CATEGORY,
          "iucnRedListCategoryCode",
          EventSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_ISSUE,
          "issues");
}
