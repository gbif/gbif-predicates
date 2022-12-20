package org.gbif.predicate.query.occurrence;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Map;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.predicate.SimplePredicate;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GadmTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.predicate.query.SQLTermsMapper;

public class OccurrenceTermsMapper implements SQLTermsMapper<OccurrenceSearchParameter> {
  // parameters that map directly to Hive.
  // boundingBox, coordinate, taxonKey and gadmGid are treated specially!
  private static final Map<OccurrenceSearchParameter, ? extends Term> PARAM_TO_TERM =
      ImmutableMap.<OccurrenceSearchParameter, Term>builder()
          .put(OccurrenceSearchParameter.DATASET_KEY, GbifTerm.datasetKey)
          .put(OccurrenceSearchParameter.YEAR, DwcTerm.year)
          .put(OccurrenceSearchParameter.MONTH, DwcTerm.month)
          .put(OccurrenceSearchParameter.DECIMAL_LATITUDE, DwcTerm.decimalLatitude)
          .put(OccurrenceSearchParameter.DECIMAL_LONGITUDE, DwcTerm.decimalLongitude)
          .put(OccurrenceSearchParameter.ELEVATION, GbifTerm.elevation)
          .put(OccurrenceSearchParameter.DEPTH, GbifTerm.depth)
          .put(OccurrenceSearchParameter.INSTITUTION_CODE, DwcTerm.institutionCode)
          .put(OccurrenceSearchParameter.COLLECTION_CODE, DwcTerm.collectionCode)
          .put(OccurrenceSearchParameter.CATALOG_NUMBER, DwcTerm.catalogNumber)
          .put(OccurrenceSearchParameter.SCIENTIFIC_NAME, DwcTerm.scientificName)
          .put(OccurrenceSearchParameter.OCCURRENCE_ID, DwcTerm.occurrenceID)
          .put(OccurrenceSearchParameter.ESTABLISHMENT_MEANS, DwcTerm.establishmentMeans)
          .put(OccurrenceSearchParameter.DEGREE_OF_ESTABLISHMENT, DwcTerm.degreeOfEstablishment)
          .put(OccurrenceSearchParameter.PATHWAY, DwcTerm.pathway)
          // the following need some value transformation
          .put(OccurrenceSearchParameter.EVENT_DATE, DwcTerm.eventDate)
          .put(OccurrenceSearchParameter.MODIFIED, DcTerm.modified)
          .put(OccurrenceSearchParameter.LAST_INTERPRETED, GbifTerm.lastInterpreted)
          .put(OccurrenceSearchParameter.BASIS_OF_RECORD, DwcTerm.basisOfRecord)
          .put(OccurrenceSearchParameter.COUNTRY, DwcTerm.countryCode)
          .put(OccurrenceSearchParameter.CONTINENT, DwcTerm.continent)
          .put(OccurrenceSearchParameter.PUBLISHING_COUNTRY, GbifTerm.publishingCountry)
          .put(OccurrenceSearchParameter.RECORDED_BY, DwcTerm.recordedBy)
          .put(OccurrenceSearchParameter.IDENTIFIED_BY, DwcTerm.identifiedBy)
          .put(OccurrenceSearchParameter.RECORD_NUMBER, DwcTerm.recordNumber)
          .put(OccurrenceSearchParameter.TYPE_STATUS, DwcTerm.typeStatus)
          .put(OccurrenceSearchParameter.HAS_COORDINATE, GbifTerm.hasCoordinate)
          .put(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE, GbifTerm.hasGeospatialIssues)
          .put(OccurrenceSearchParameter.MEDIA_TYPE, GbifTerm.mediaType)
          .put(OccurrenceSearchParameter.ISSUE, GbifTerm.issue)
          .put(OccurrenceSearchParameter.KINGDOM_KEY, GbifTerm.kingdomKey)
          .put(OccurrenceSearchParameter.PHYLUM_KEY, GbifTerm.phylumKey)
          .put(OccurrenceSearchParameter.CLASS_KEY, GbifTerm.classKey)
          .put(OccurrenceSearchParameter.ORDER_KEY, GbifTerm.orderKey)
          .put(OccurrenceSearchParameter.FAMILY_KEY, GbifTerm.familyKey)
          .put(OccurrenceSearchParameter.GENUS_KEY, GbifTerm.genusKey)
          .put(OccurrenceSearchParameter.SUBGENUS_KEY, GbifTerm.subgenusKey)
          .put(OccurrenceSearchParameter.SPECIES_KEY, GbifTerm.speciesKey)
          .put(OccurrenceSearchParameter.ACCEPTED_TAXON_KEY, GbifTerm.acceptedTaxonKey)
          .put(OccurrenceSearchParameter.TAXONOMIC_STATUS, DwcTerm.taxonomicStatus)
          .put(OccurrenceSearchParameter.REPATRIATED, GbifTerm.repatriated)
          .put(OccurrenceSearchParameter.ORGANISM_ID, DwcTerm.organismID)
          .put(OccurrenceSearchParameter.LOCALITY, DwcTerm.locality)
          .put(
              OccurrenceSearchParameter.COORDINATE_UNCERTAINTY_IN_METERS,
              DwcTerm.coordinateUncertaintyInMeters)
          .put(OccurrenceSearchParameter.STATE_PROVINCE, DwcTerm.stateProvince)
          .put(OccurrenceSearchParameter.WATER_BODY, DwcTerm.waterBody)
          .put(OccurrenceSearchParameter.GADM_LEVEL_0_GID, GadmTerm.level0Gid)
          .put(OccurrenceSearchParameter.GADM_LEVEL_1_GID, GadmTerm.level1Gid)
          .put(OccurrenceSearchParameter.GADM_LEVEL_2_GID, GadmTerm.level2Gid)
          .put(OccurrenceSearchParameter.GADM_LEVEL_3_GID, GadmTerm.level3Gid)
          .put(OccurrenceSearchParameter.PROTOCOL, GbifTerm.protocol)
          .put(OccurrenceSearchParameter.LICENSE, DcTerm.license)
          .put(OccurrenceSearchParameter.PUBLISHING_ORG, GbifInternalTerm.publishingOrgKey)
          .put(
              OccurrenceSearchParameter.HOSTING_ORGANIZATION_KEY,
              GbifInternalTerm.hostingOrganizationKey)
          .put(OccurrenceSearchParameter.CRAWL_ID, GbifInternalTerm.crawlId)
          .put(OccurrenceSearchParameter.INSTALLATION_KEY, GbifInternalTerm.installationKey)
          .put(OccurrenceSearchParameter.NETWORK_KEY, GbifInternalTerm.networkKey)
          .put(OccurrenceSearchParameter.EVENT_ID, DwcTerm.eventID)
          .put(OccurrenceSearchParameter.PARENT_EVENT_ID, DwcTerm.parentEventID)
          .put(OccurrenceSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol)
          .put(OccurrenceSearchParameter.PROJECT_ID, GbifInternalTerm.projectId)
          .put(OccurrenceSearchParameter.PROGRAMME, GbifInternalTerm.programmeAcronym)
          .put(OccurrenceSearchParameter.VERBATIM_SCIENTIFIC_NAME, GbifTerm.verbatimScientificName)
          .put(OccurrenceSearchParameter.TAXON_ID, DwcTerm.taxonID)
          .put(OccurrenceSearchParameter.SAMPLE_SIZE_UNIT, DwcTerm.sampleSizeUnit)
          .put(OccurrenceSearchParameter.SAMPLE_SIZE_VALUE, DwcTerm.sampleSizeValue)
          .put(OccurrenceSearchParameter.ORGANISM_QUANTITY, DwcTerm.organismQuantity)
          .put(OccurrenceSearchParameter.ORGANISM_QUANTITY_TYPE, DwcTerm.organismQuantityType)
          .put(
              OccurrenceSearchParameter.RELATIVE_ORGANISM_QUANTITY,
              GbifTerm.relativeOrganismQuantity)
          .put(OccurrenceSearchParameter.COLLECTION_KEY, GbifInternalTerm.collectionKey)
          .put(OccurrenceSearchParameter.INSTITUTION_KEY, GbifInternalTerm.institutionKey)
          .put(OccurrenceSearchParameter.RECORDED_BY_ID, DwcTerm.recordedByID)
          .put(OccurrenceSearchParameter.IDENTIFIED_BY_ID, DwcTerm.identifiedByID)
          .put(OccurrenceSearchParameter.OCCURRENCE_STATUS, DwcTerm.occurrenceStatus)
          .put(OccurrenceSearchParameter.LIFE_STAGE, DwcTerm.lifeStage)
          .put(OccurrenceSearchParameter.IS_IN_CLUSTER, GbifInternalTerm.isInCluster)
          .put(OccurrenceSearchParameter.DWCA_EXTENSION, GbifInternalTerm.dwcaExtension)
          .put(OccurrenceSearchParameter.IUCN_RED_LIST_CATEGORY, IucnTerm.iucnRedListCategory)
          .put(OccurrenceSearchParameter.DATASET_ID, DwcTerm.datasetID)
          .put(OccurrenceSearchParameter.DATASET_NAME, DwcTerm.datasetName)
          .put(OccurrenceSearchParameter.OTHER_CATALOG_NUMBERS, DwcTerm.otherCatalogNumbers)
          .put(OccurrenceSearchParameter.PREPARATIONS, DwcTerm.preparations)
          .put(
              OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS,
              GbifTerm.distanceFromCentroidInMeters)
          .build();

  private static final Map<OccurrenceSearchParameter, Term> ARRAY_STRING_TERMS =
      ImmutableMap.<OccurrenceSearchParameter, Term>builder()
          .put(OccurrenceSearchParameter.NETWORK_KEY, GbifInternalTerm.networkKey)
          .put(OccurrenceSearchParameter.DWCA_EXTENSION, GbifInternalTerm.dwcaExtension)
          .put(OccurrenceSearchParameter.IDENTIFIED_BY_ID, DwcTerm.identifiedByID)
          .put(OccurrenceSearchParameter.RECORDED_BY_ID, DwcTerm.recordedByID)
          .put(OccurrenceSearchParameter.DATASET_ID, DwcTerm.datasetID)
          .put(OccurrenceSearchParameter.DATASET_NAME, DwcTerm.datasetName)
          .put(OccurrenceSearchParameter.OTHER_CATALOG_NUMBERS, DwcTerm.otherCatalogNumbers)
          .put(OccurrenceSearchParameter.RECORDED_BY, DwcTerm.recordedBy)
          .put(OccurrenceSearchParameter.IDENTIFIED_BY, DwcTerm.identifiedBy)
          .put(OccurrenceSearchParameter.PREPARATIONS, DwcTerm.preparations)
          .put(OccurrenceSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol)
          .build();

  private static final Map<OccurrenceSearchParameter, Term> DENORMED_TERMS = Collections.emptyMap();

  // reserved hive words
  public static final ImmutableSet<String> HIVE_RESERVED_WORDS =
      new ImmutableSet.Builder<String>().add("date", "order", "format", "group").build();

  @Override
  public Term term(OccurrenceSearchParameter searchParameter) {
    return PARAM_TO_TERM.get(searchParameter);
  }

  @Override
  public boolean isArray(OccurrenceSearchParameter searchParameter) {
    return ARRAY_STRING_TERMS.containsKey(searchParameter);
  }

  @Override
  public Term getTermArray(OccurrenceSearchParameter searchParameter) {
    return ARRAY_STRING_TERMS.get(searchParameter);
  }

  @Override
  public boolean isDenormedTerm(OccurrenceSearchParameter searchParameter) {
    return DENORMED_TERMS.containsKey(searchParameter);
  }

  @Override
  public OccurrenceSearchParameter getDefaultGadmLevel() {
    return OccurrenceSearchParameter.GADM_LEVEL_0_GID;
  }

  @Override
  public boolean includeNullInPredicate(SimplePredicate<OccurrenceSearchParameter> predicate) {
    return OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS == predicate.getKey()
        && predicate instanceof GreaterThanOrEqualsPredicate;
  }
}
