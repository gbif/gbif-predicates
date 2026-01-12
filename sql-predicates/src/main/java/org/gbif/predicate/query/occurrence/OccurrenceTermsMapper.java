package org.gbif.predicate.query.occurrence;

import java.util.Collections;
import java.util.Map;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.occurrence.search.InternalOccurrenceSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.predicate.GreaterThanPredicate;
import org.gbif.api.model.predicate.SimplePredicate;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.dwc.terms.GadmTerm;
import org.gbif.dwc.terms.GbifInternalTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.IucnTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.predicate.query.SQLTermsMapper;

public class OccurrenceTermsMapper implements SQLTermsMapper<SearchParameter> {

  // TODO: humboldt

  // parameters that map directly to Hive.
  // boundingBox, coordinate, taxonKey and gadmGid are treated specially!
  private static final Map<SearchParameter, Term> PARAM_TO_TERM =
      Map.ofEntries(
          Map.entry(OccurrenceSearchParameter.DATASET_KEY, GbifTerm.datasetKey),
          Map.entry(OccurrenceSearchParameter.SEX, DwcTerm.sex),
          Map.entry(OccurrenceSearchParameter.YEAR, DwcTerm.year),
          Map.entry(OccurrenceSearchParameter.MONTH, DwcTerm.month),
          Map.entry(OccurrenceSearchParameter.DAY, DwcTerm.day),
          Map.entry(OccurrenceSearchParameter.DECIMAL_LATITUDE, DwcTerm.decimalLatitude),
          Map.entry(OccurrenceSearchParameter.DECIMAL_LONGITUDE, DwcTerm.decimalLongitude),
          Map.entry(OccurrenceSearchParameter.ELEVATION, GbifTerm.elevation),
          Map.entry(OccurrenceSearchParameter.DEPTH, GbifTerm.depth),
          Map.entry(OccurrenceSearchParameter.INSTITUTION_CODE, DwcTerm.institutionCode),
          Map.entry(OccurrenceSearchParameter.COLLECTION_CODE, DwcTerm.collectionCode),
          Map.entry(OccurrenceSearchParameter.CATALOG_NUMBER, DwcTerm.catalogNumber),
          Map.entry(OccurrenceSearchParameter.SCIENTIFIC_NAME, DwcTerm.scientificName),
          Map.entry(OccurrenceSearchParameter.OCCURRENCE_ID, DwcTerm.occurrenceID),
          Map.entry(OccurrenceSearchParameter.ESTABLISHMENT_MEANS, DwcTerm.establishmentMeans),
          Map.entry(
              OccurrenceSearchParameter.DEGREE_OF_ESTABLISHMENT, DwcTerm.degreeOfEstablishment),
          Map.entry(OccurrenceSearchParameter.PATHWAY, DwcTerm.pathway),
          Map.entry(OccurrenceSearchParameter.EVENT_DATE, DwcTerm.eventDate),
          Map.entry(OccurrenceSearchParameter.START_DAY_OF_YEAR, DwcTerm.startDayOfYear),
          Map.entry(OccurrenceSearchParameter.END_DAY_OF_YEAR, DwcTerm.endDayOfYear),
          Map.entry(OccurrenceSearchParameter.MODIFIED, DcTerm.modified),
          Map.entry(OccurrenceSearchParameter.LAST_INTERPRETED, GbifTerm.lastInterpreted),
          Map.entry(OccurrenceSearchParameter.BASIS_OF_RECORD, DwcTerm.basisOfRecord),
          Map.entry(OccurrenceSearchParameter.COUNTRY, DwcTerm.countryCode),
          Map.entry(OccurrenceSearchParameter.CONTINENT, DwcTerm.continent),
          Map.entry(OccurrenceSearchParameter.PUBLISHING_COUNTRY, GbifTerm.publishingCountry),
          Map.entry(OccurrenceSearchParameter.RECORDED_BY, DwcTerm.recordedBy),
          Map.entry(OccurrenceSearchParameter.IDENTIFIED_BY, DwcTerm.identifiedBy),
          Map.entry(OccurrenceSearchParameter.RECORD_NUMBER, DwcTerm.recordNumber),
          Map.entry(OccurrenceSearchParameter.TYPE_STATUS, DwcTerm.typeStatus),
          Map.entry(OccurrenceSearchParameter.HAS_COORDINATE, GbifTerm.hasCoordinate),
          Map.entry(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE, GbifTerm.hasGeospatialIssues),
          Map.entry(OccurrenceSearchParameter.MEDIA_TYPE, GbifTerm.mediaType),
          Map.entry(OccurrenceSearchParameter.ISSUE, GbifTerm.issue),
          Map.entry(OccurrenceSearchParameter.TAXONOMIC_ISSUE, GbifTerm.taxonomicIssue),
          Map.entry(OccurrenceSearchParameter.KINGDOM_KEY, GbifTerm.kingdomKey),
          Map.entry(OccurrenceSearchParameter.PHYLUM_KEY, GbifTerm.phylumKey),
          Map.entry(OccurrenceSearchParameter.CLASS_KEY, GbifTerm.classKey),
          Map.entry(OccurrenceSearchParameter.ORDER_KEY, GbifTerm.orderKey),
          Map.entry(OccurrenceSearchParameter.FAMILY_KEY, GbifTerm.familyKey),
          Map.entry(OccurrenceSearchParameter.GENUS_KEY, GbifTerm.genusKey),
          Map.entry(OccurrenceSearchParameter.SUBGENUS_KEY, GbifTerm.subgenusKey),
          Map.entry(OccurrenceSearchParameter.SPECIES_KEY, GbifTerm.speciesKey),
          Map.entry(OccurrenceSearchParameter.ACCEPTED_TAXON_KEY, GbifTerm.acceptedTaxonKey),
          Map.entry(OccurrenceSearchParameter.TAXONOMIC_STATUS, DwcTerm.taxonomicStatus),
          Map.entry(OccurrenceSearchParameter.REPATRIATED, GbifTerm.repatriated),
          Map.entry(OccurrenceSearchParameter.ORGANISM_ID, DwcTerm.organismID),
          Map.entry(OccurrenceSearchParameter.LOCALITY, DwcTerm.locality),
          Map.entry(
              OccurrenceSearchParameter.COORDINATE_UNCERTAINTY_IN_METERS,
              DwcTerm.coordinateUncertaintyInMeters),
          Map.entry(OccurrenceSearchParameter.STATE_PROVINCE, DwcTerm.stateProvince),
          Map.entry(OccurrenceSearchParameter.WATER_BODY, DwcTerm.waterBody),
          Map.entry(OccurrenceSearchParameter.GADM_LEVEL_0_GID, GadmTerm.level0Gid),
          Map.entry(OccurrenceSearchParameter.GADM_LEVEL_1_GID, GadmTerm.level1Gid),
          Map.entry(OccurrenceSearchParameter.GADM_LEVEL_2_GID, GadmTerm.level2Gid),
          Map.entry(OccurrenceSearchParameter.GADM_LEVEL_3_GID, GadmTerm.level3Gid),
          Map.entry(OccurrenceSearchParameter.PROTOCOL, GbifTerm.protocol),
          Map.entry(OccurrenceSearchParameter.LICENSE, DcTerm.license),
          Map.entry(OccurrenceSearchParameter.PUBLISHING_ORG, GbifInternalTerm.publishingOrgKey),
          Map.entry(
              OccurrenceSearchParameter.HOSTING_ORGANIZATION_KEY,
              GbifInternalTerm.hostingOrganizationKey),
          Map.entry(OccurrenceSearchParameter.CRAWL_ID, GbifInternalTerm.crawlId),
          Map.entry(OccurrenceSearchParameter.INSTALLATION_KEY, GbifInternalTerm.installationKey),
          Map.entry(OccurrenceSearchParameter.NETWORK_KEY, GbifInternalTerm.networkKey),
          Map.entry(OccurrenceSearchParameter.EVENT_ID, DwcTerm.eventID),
          Map.entry(OccurrenceSearchParameter.PARENT_EVENT_ID, DwcTerm.parentEventID),
          Map.entry(OccurrenceSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol),
          Map.entry(OccurrenceSearchParameter.PROGRAMME, GbifInternalTerm.programmeAcronym),
          Map.entry(
              OccurrenceSearchParameter.VERBATIM_SCIENTIFIC_NAME, GbifTerm.verbatimScientificName),
          Map.entry(OccurrenceSearchParameter.TAXON_ID, DwcTerm.taxonID),
          Map.entry(OccurrenceSearchParameter.TAXON_CONCEPT_ID, DwcTerm.taxonConceptID),
          Map.entry(OccurrenceSearchParameter.SAMPLE_SIZE_UNIT, DwcTerm.sampleSizeUnit),
          Map.entry(OccurrenceSearchParameter.SAMPLE_SIZE_VALUE, DwcTerm.sampleSizeValue),
          Map.entry(
              OccurrenceSearchParameter.PREVIOUS_IDENTIFICATIONS, DwcTerm.previousIdentifications),
          Map.entry(OccurrenceSearchParameter.ORGANISM_QUANTITY, DwcTerm.organismQuantity),
          Map.entry(OccurrenceSearchParameter.ORGANISM_QUANTITY_TYPE, DwcTerm.organismQuantityType),
          Map.entry(
              OccurrenceSearchParameter.RELATIVE_ORGANISM_QUANTITY,
              GbifTerm.relativeOrganismQuantity),
          Map.entry(OccurrenceSearchParameter.COLLECTION_KEY, GbifInternalTerm.collectionKey),
          Map.entry(OccurrenceSearchParameter.INSTITUTION_KEY, GbifInternalTerm.institutionKey),
          Map.entry(OccurrenceSearchParameter.RECORDED_BY_ID, DwcTerm.recordedByID),
          Map.entry(OccurrenceSearchParameter.IDENTIFIED_BY_ID, DwcTerm.identifiedByID),
          Map.entry(OccurrenceSearchParameter.OCCURRENCE_STATUS, DwcTerm.occurrenceStatus),
          Map.entry(OccurrenceSearchParameter.LIFE_STAGE, DwcTerm.lifeStage),
          Map.entry(OccurrenceSearchParameter.IS_IN_CLUSTER, GbifInternalTerm.isInCluster),
          Map.entry(OccurrenceSearchParameter.DWCA_EXTENSION, GbifInternalTerm.dwcaExtension),
          Map.entry(OccurrenceSearchParameter.IUCN_RED_LIST_CATEGORY, IucnTerm.iucnRedListCategory),
          Map.entry(OccurrenceSearchParameter.DATASET_ID, DwcTerm.datasetID),
          Map.entry(OccurrenceSearchParameter.DATASET_NAME, DwcTerm.datasetName),
          Map.entry(OccurrenceSearchParameter.OTHER_CATALOG_NUMBERS, DwcTerm.otherCatalogNumbers),
          Map.entry(OccurrenceSearchParameter.PREPARATIONS, DwcTerm.preparations),
          Map.entry(
              OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS,
              GbifTerm.distanceFromCentroidInMeters),
          Map.entry(OccurrenceSearchParameter.GBIF_ID, GbifTerm.gbifID),
          Map.entry(
              InternalOccurrenceSearchParameter.EVENT_DATE_GTE, GbifInternalTerm.eventDateGte),
          Map.entry(
              InternalOccurrenceSearchParameter.EVENT_DATE_LTE, GbifInternalTerm.eventDateLte),
          Map.entry(OccurrenceSearchParameter.GBIF_REGION, GbifTerm.gbifRegion),
          Map.entry(
              OccurrenceSearchParameter.PUBLISHED_BY_GBIF_REGION, GbifTerm.publishedByGbifRegion),
          Map.entry(OccurrenceSearchParameter.IS_SEQUENCED, GbifTerm.isSequenced),
          Map.entry(OccurrenceSearchParameter.ISLAND, DwcTerm.island),
          Map.entry(OccurrenceSearchParameter.ISLAND_GROUP, DwcTerm.islandGroup),
          Map.entry(OccurrenceSearchParameter.FIELD_NUMBER, DwcTerm.fieldNumber),
          Map.entry(
              OccurrenceSearchParameter.EARLIEST_EON_OR_LOWEST_EONOTHEM,
              DwcTerm.earliestEonOrLowestEonothem),
          Map.entry(
              OccurrenceSearchParameter.LATEST_EON_OR_HIGHEST_EONOTHEM,
              DwcTerm.latestEonOrHighestEonothem),
          Map.entry(
              OccurrenceSearchParameter.EARLIEST_ERA_OR_LOWEST_ERATHEM,
              DwcTerm.earliestEraOrLowestErathem),
          Map.entry(
              OccurrenceSearchParameter.LATEST_ERA_OR_HIGHEST_ERATHEM,
              DwcTerm.latestEraOrHighestErathem),
          Map.entry(
              OccurrenceSearchParameter.EARLIEST_PERIOD_OR_LOWEST_SYSTEM,
              DwcTerm.earliestPeriodOrLowestSystem),
          Map.entry(
              OccurrenceSearchParameter.LATEST_PERIOD_OR_HIGHEST_SYSTEM,
              DwcTerm.latestPeriodOrHighestSystem),
          Map.entry(
              OccurrenceSearchParameter.EARLIEST_EPOCH_OR_LOWEST_SERIES,
              DwcTerm.earliestEpochOrLowestSeries),
          Map.entry(
              OccurrenceSearchParameter.LATEST_EPOCH_OR_HIGHEST_SERIES,
              DwcTerm.latestEpochOrHighestSeries),
          Map.entry(
              OccurrenceSearchParameter.EARLIEST_AGE_OR_LOWEST_STAGE,
              DwcTerm.earliestAgeOrLowestStage),
          Map.entry(
              OccurrenceSearchParameter.LATEST_AGE_OR_HIGHEST_STAGE,
              DwcTerm.latestAgeOrHighestStage),
          Map.entry(
              OccurrenceSearchParameter.LOWEST_BIOSTRATIGRAPHIC_ZONE,
              DwcTerm.lowestBiostratigraphicZone),
          Map.entry(
              OccurrenceSearchParameter.HIGHEST_BIOSTRATIGRAPHIC_ZONE,
              DwcTerm.highestBiostratigraphicZone),
          Map.entry(OccurrenceSearchParameter.GROUP, DwcTerm.group),
          Map.entry(OccurrenceSearchParameter.FORMATION, DwcTerm.formation),
          Map.entry(OccurrenceSearchParameter.MEMBER, DwcTerm.member),
          Map.entry(OccurrenceSearchParameter.BED, DwcTerm.bed),
          Map.entry(OccurrenceSearchParameter.PROJECT_ID, GbifTerm.projectId),
          Map.entry(OccurrenceSearchParameter.CHECKLIST_KEY, GbifTerm.checklistKey),
          Map.entry(OccurrenceSearchParameter.GEOLOGICAL_TIME, GbifTerm.geologicalTime),
          Map.entry(OccurrenceSearchParameter.LITHOSTRATIGRAPHY, GbifTerm.lithostratigraphy),
          Map.entry(OccurrenceSearchParameter.BIOSTRATIGRAPHY, GbifTerm.biostratigraphy),
          Map.entry(OccurrenceSearchParameter.EVENT_DATE_GTE, GbifInternalTerm.eventDateGte),
          Map.entry(OccurrenceSearchParameter.DNA_SEQUENCE_ID, GbifTerm.dnaSequenceID),

          // Humboldt
          Map.entry(EventSearchParameter.HUMBOLDT_SITE_COUNT, EcoTerm.siteCount),
          Map.entry(EventSearchParameter.HUMBOLDT_VERBATIM_SITE_NAMES, EcoTerm.verbatimSiteNames),
          Map.entry(
              EventSearchParameter.HUMBOLDT_GEOSPATIAL_SCOPE_AREA_VALUE,
              EcoTerm.geospatialScopeAreaValue),
          Map.entry(
              EventSearchParameter.HUMBOLDT_GEOSPATIAL_SCOPE_AREA_UNIT,
              EcoTerm.geospatialScopeAreaUnit),
          Map.entry(
              EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_VALUE,
              EcoTerm.totalAreaSampledValue),
          Map.entry(
              EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT, EcoTerm.totalAreaSampledUnit),
          Map.entry(EventSearchParameter.HUMBOLDT_TARGET_HABITAT_SCOPE, EcoTerm.targetHabitatScope),
          Map.entry(
              EventSearchParameter.HUMBOLDT_EVENT_DURATION,
              GbifInternalTerm.humboldtEventDurationValueInMinutes),
          Map.entry(
              EventSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES,
              GbifInternalTerm.humboldtEventDurationValueInMinutes),
          Map.entry(EventSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE, EcoTerm.eventDurationValue),
          Map.entry(EventSearchParameter.HUMBOLDT_EVENT_DURATION_UNIT, EcoTerm.eventDurationUnit),
          Map.entry(
              EventSearchParameter.HUMBOLDT_TAXON_COMPLETENESS_PROTOCOLS,
              EcoTerm.taxonCompletenessProtocols),
          Map.entry(
              EventSearchParameter.HUMBOLDT_IS_TAXONOMIC_SCOPE_FULLY_REPORTED,
              EcoTerm.isTaxonomicScopeFullyReported),
          Map.entry(EventSearchParameter.HUMBOLDT_IS_ABSENCE_REPORTED, EcoTerm.isAbsenceReported),
          Map.entry(EventSearchParameter.HUMBOLDT_HAS_NON_TARGET_TAXA, EcoTerm.hasNonTargetTaxa),
          Map.entry(
              EventSearchParameter.HUMBOLDT_ARE_NON_TARGET_TAXA_FULLY_REPORTED,
              EcoTerm.areNonTargetTaxaFullyReported),
          Map.entry(
              EventSearchParameter.HUMBOLDT_TARGET_LIFE_STAGE_SCOPE, EcoTerm.targetLifeStageScope),
          Map.entry(
              EventSearchParameter.HUMBOLDT_IS_LIFE_STAGE_SCOPE_FULLY_REPORTED,
              EcoTerm.isLifeStageScopeFullyReported),
          Map.entry(
              EventSearchParameter.HUMBOLDT_TARGET_DEGREE_OF_ESTABLISHMENT_SCOPE,
              EcoTerm.targetDegreeOfEstablishmentScope),
          Map.entry(
              EventSearchParameter.HUMBOLDT_IS_DEGREE_OF_ESTABLISHMENT_SCOPE_FULLY_REPORTED,
              EcoTerm.isDegreeOfEstablishmentScopeFullyReported),
          Map.entry(
              EventSearchParameter.HUMBOLDT_TARGET_GROWTH_FORM_SCOPE,
              EcoTerm.targetGrowthFormScope),
          Map.entry(
              EventSearchParameter.HUMBOLDT_IS_GROWTH_FORM_SCOPE_FULLY_REPORTED,
              EcoTerm.isGrowthFormScopeFullyReported),
          Map.entry(
              EventSearchParameter.HUMBOLDT_HAS_NON_TARGET_ORGANISMS,
              EcoTerm.hasNonTargetOrganisms),
          Map.entry(EventSearchParameter.HUMBOLDT_COMPILATION_TYPES, EcoTerm.compilationTypes),
          Map.entry(
              EventSearchParameter.HUMBOLDT_COMPILATION_SOURCE_TYPES,
              EcoTerm.compilationSourceTypes),
          Map.entry(EventSearchParameter.HUMBOLDT_INVENTORY_TYPES, EcoTerm.inventoryTypes),
          Map.entry(EventSearchParameter.HUMBOLDT_PROTOCOL_NAMES, EcoTerm.protocolNames),
          Map.entry(
              EventSearchParameter.HUMBOLDT_IS_ABUNDANCE_REPORTED, EcoTerm.isAbundanceReported),
          Map.entry(
              EventSearchParameter.HUMBOLDT_IS_ABUNDANCE_CAP_REPORTED,
              EcoTerm.isAbundanceCapReported),
          Map.entry(EventSearchParameter.HUMBOLDT_ABUNDANCE_CAP, EcoTerm.abundanceCap),
          Map.entry(
              EventSearchParameter.HUMBOLDT_IS_VEGETATION_COVER_REPORTED,
              EcoTerm.isVegetationCoverReported),
          Map.entry(
              EventSearchParameter.HUMBOLDT_IS_LEAST_SPECIFIC_TARGET_CATEGORY_QUANTITY_INCLUSIVE,
              EcoTerm.isLeastSpecificTargetCategoryQuantityInclusive),
          Map.entry(EventSearchParameter.HUMBOLDT_HAS_VOUCHERS, EcoTerm.hasVouchers),
          Map.entry(
              EventSearchParameter.HUMBOLDT_VOUCHER_INSTITUTIONS, EcoTerm.voucherInstitutions),
          Map.entry(EventSearchParameter.HUMBOLDT_HAS_MATERIAL_SAMPLES, EcoTerm.hasMaterialSamples),
          Map.entry(
              EventSearchParameter.HUMBOLDT_MATERIAL_SAMPLE_TYPES, EcoTerm.materialSampleTypes),
          Map.entry(
              EventSearchParameter.HUMBOLDT_SAMPLING_PERFORMED_BY, EcoTerm.samplingPerformedBy),
          Map.entry(
              EventSearchParameter.HUMBOLDT_IS_SAMPLING_EFFORT_REPORTED,
              EcoTerm.isSamplingEffortReported),
          Map.entry(
              EventSearchParameter.HUMBOLDT_SAMPLING_EFFORT_VALUE, EcoTerm.samplingEffortValue),
          Map.entry(
              EventSearchParameter.HUMBOLDT_SAMPLING_EFFORT_UNIT, EcoTerm.samplingEffortUnit));

  private static final Map<OccurrenceSearchParameter, Term> ARRAY_STRING_TERMS =
      Map.ofEntries(
          Map.entry(OccurrenceSearchParameter.NETWORK_KEY, GbifInternalTerm.networkKey),
          Map.entry(OccurrenceSearchParameter.DWCA_EXTENSION, GbifInternalTerm.dwcaExtension),
          Map.entry(OccurrenceSearchParameter.IDENTIFIED_BY_ID, DwcTerm.identifiedByID),
          Map.entry(OccurrenceSearchParameter.RECORDED_BY_ID, DwcTerm.recordedByID),
          Map.entry(OccurrenceSearchParameter.DATASET_ID, DwcTerm.datasetID),
          Map.entry(OccurrenceSearchParameter.DATASET_NAME, DwcTerm.datasetName),
          Map.entry(OccurrenceSearchParameter.OTHER_CATALOG_NUMBERS, DwcTerm.otherCatalogNumbers),
          Map.entry(OccurrenceSearchParameter.RECORDED_BY, DwcTerm.recordedBy),
          Map.entry(OccurrenceSearchParameter.IDENTIFIED_BY, DwcTerm.identifiedBy),
          Map.entry(OccurrenceSearchParameter.PREPARATIONS, DwcTerm.preparations),
          Map.entry(OccurrenceSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol),
          Map.entry(OccurrenceSearchParameter.PROJECT_ID, GbifTerm.projectId),
          Map.entry(OccurrenceSearchParameter.GEOREFERENCED_BY, DwcTerm.georeferencedBy),
          Map.entry(OccurrenceSearchParameter.HIGHER_GEOGRAPHY, DwcTerm.higherGeography),
          Map.entry(OccurrenceSearchParameter.ASSOCIATED_SEQUENCES, DwcTerm.associatedSequences),
          Map.entry(OccurrenceSearchParameter.DNA_SEQUENCE_ID, GbifTerm.dnaSequenceID));

  private static final Map<SearchParameter, Term> DENORMED_TERMS = Collections.emptyMap();

  @Override
  public Term term(SearchParameter searchParameter) {
    return PARAM_TO_TERM.get(searchParameter);
  }

  @Override
  public boolean isArray(SearchParameter searchParameter) {
    return ARRAY_STRING_TERMS.containsKey(searchParameter);
  }

  @Override
  public Term getTermArray(SearchParameter searchParameter) {
    return ARRAY_STRING_TERMS.get(searchParameter);
  }

  @Override
  public boolean isDenormedTerm(SearchParameter searchParameter) {
    return DENORMED_TERMS.containsKey(searchParameter);
  }

  @Override
  public SearchParameter getDefaultGadmLevel() {
    return OccurrenceSearchParameter.GADM_LEVEL_0_GID;
  }

  @Override
  public boolean includeNullInPredicate(SimplePredicate<SearchParameter> predicate) {
    return OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS == predicate.getKey()
        && (predicate instanceof GreaterThanOrEqualsPredicate
            || predicate instanceof GreaterThanPredicate);
  }
}
