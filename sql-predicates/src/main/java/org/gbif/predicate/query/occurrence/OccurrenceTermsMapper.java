package org.gbif.predicate.query.occurrence;

import com.google.common.collect.ImmutableMap;
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

  static Map<? extends SearchParameter, ? extends Term> EVENTS_PARAM_TO_TERM =
      ImmutableMap.<EventSearchParameter, Term>builder()
          .put(EventSearchParameter.DATASET_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.CHECKLIST_KEY, GbifTerm.checklistKey)
          .put(EventSearchParameter.YEAR, GbifTerm.datasetKey)
          .put(EventSearchParameter.MONTH, GbifTerm.datasetKey)
          .put(EventSearchParameter.DAY, GbifTerm.datasetKey)
          .put(EventSearchParameter.START_DAY_OF_YEAR, GbifTerm.datasetKey)
          .put(EventSearchParameter.END_DAY_OF_YEAR, GbifTerm.datasetKey)
          .put(EventSearchParameter.EVENT_DATE, GbifTerm.datasetKey)
          .put(EventSearchParameter.EVENT_DATE_GTE, GbifTerm.datasetKey)
          .put(EventSearchParameter.EVENT_ID, GbifTerm.datasetKey)
          .put(EventSearchParameter.PARENT_EVENT_ID, GbifTerm.datasetKey)
          .put(EventSearchParameter.SAMPLING_PROTOCOL, GbifTerm.datasetKey)
          .put(EventSearchParameter.PREVIOUS_IDENTIFICATIONS, GbifTerm.datasetKey)
          .put(EventSearchParameter.LAST_INTERPRETED, GbifTerm.datasetKey)
          .put(EventSearchParameter.MODIFIED, GbifTerm.datasetKey)
          .put(EventSearchParameter.DECIMAL_LATITUDE, GbifTerm.datasetKey)
          .put(EventSearchParameter.DECIMAL_LONGITUDE, GbifTerm.datasetKey)
          .put(EventSearchParameter.COORDINATE_UNCERTAINTY_IN_METERS, GbifTerm.datasetKey)
          .put(EventSearchParameter.COUNTRY, GbifTerm.datasetKey)
          .put(EventSearchParameter.GBIF_REGION, GbifTerm.datasetKey)
          .put(EventSearchParameter.CONTINENT, GbifTerm.datasetKey)
          .put(EventSearchParameter.PUBLISHING_COUNTRY, GbifTerm.datasetKey)
          .put(EventSearchParameter.PUBLISHED_BY_GBIF_REGION, GbifTerm.datasetKey)
          .put(EventSearchParameter.ELEVATION, GbifTerm.datasetKey)
          .put(EventSearchParameter.DEPTH, GbifTerm.datasetKey)
          .put(EventSearchParameter.INSTITUTION_CODE, GbifTerm.datasetKey)
          .put(EventSearchParameter.COLLECTION_CODE, GbifTerm.datasetKey)
          .put(EventSearchParameter.TAXON_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.ACCEPTED_TAXON_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.KINGDOM_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.PHYLUM_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.CLASS_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.ORDER_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.FAMILY_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.GENUS_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.SUBGENUS_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.SPECIES_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.SCIENTIFIC_NAME, GbifTerm.datasetKey)
          .put(EventSearchParameter.TAXON_ID, GbifTerm.datasetKey)
          .put(EventSearchParameter.TAXONOMIC_STATUS, GbifTerm.datasetKey)
          .put(EventSearchParameter.TAXONOMIC_ISSUE, GbifTerm.datasetKey)
          .put(EventSearchParameter.IUCN_RED_LIST_CATEGORY, GbifTerm.datasetKey)
          .put(EventSearchParameter.HAS_COORDINATE, GbifTerm.datasetKey)
          .put(EventSearchParameter.GEOMETRY, GbifTerm.datasetKey)
          .put(EventSearchParameter.GEO_DISTANCE, GbifTerm.datasetKey)
          .put(EventSearchParameter.HAS_GEOSPATIAL_ISSUE, GbifTerm.datasetKey)
          .put(EventSearchParameter.MEDIA_TYPE, GbifTerm.datasetKey)
          .put(EventSearchParameter.REPATRIATED, GbifTerm.datasetKey)
          .put(EventSearchParameter.STATE_PROVINCE, GbifTerm.datasetKey)
          .put(EventSearchParameter.WATER_BODY, GbifTerm.datasetKey)
          .put(EventSearchParameter.LOCALITY, GbifTerm.datasetKey)
          .put(EventSearchParameter.PROTOCOL, GbifTerm.datasetKey)
          .put(EventSearchParameter.LICENSE, GbifTerm.datasetKey)
          .put(EventSearchParameter.PUBLISHING_ORG, GbifTerm.datasetKey)
          .put(EventSearchParameter.NETWORK_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.INSTALLATION_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.HOSTING_ORGANIZATION_KEY, GbifTerm.datasetKey)
          .put(EventSearchParameter.CRAWL_ID, GbifTerm.datasetKey)
          .put(EventSearchParameter.PROJECT_ID, GbifTerm.datasetKey)
          .put(EventSearchParameter.PROGRAMME, GbifTerm.datasetKey)
          .put(EventSearchParameter.SAMPLE_SIZE_UNIT, GbifTerm.datasetKey)
          .put(EventSearchParameter.SAMPLE_SIZE_VALUE, GbifTerm.datasetKey)
          .put(EventSearchParameter.GADM_GID, GbifTerm.datasetKey)
          .put(EventSearchParameter.GADM_LEVEL_0_GID, GbifTerm.datasetKey)
          .put(EventSearchParameter.GADM_LEVEL_1_GID, GbifTerm.datasetKey)
          .put(EventSearchParameter.GADM_LEVEL_2_GID, GbifTerm.datasetKey)
          .put(EventSearchParameter.GADM_LEVEL_3_GID, GbifTerm.datasetKey)
          .put(EventSearchParameter.DWCA_EXTENSION, GbifTerm.datasetKey)
          .put(EventSearchParameter.DATASET_ID, GbifTerm.datasetKey)
          .put(EventSearchParameter.DATASET_NAME, GbifTerm.datasetKey)
          .put(EventSearchParameter.ISLAND, GbifTerm.datasetKey)
          .put(EventSearchParameter.ISLAND_GROUP, GbifTerm.datasetKey)
          .put(EventSearchParameter.GEOREFERENCED_BY, GbifTerm.datasetKey)
          .put(EventSearchParameter.HIGHER_GEOGRAPHY, GbifTerm.datasetKey)
          .put(EventSearchParameter.FIELD_NUMBER, GbifTerm.datasetKey)
          .put(EventSearchParameter.GBIF_ID, GbifTerm.datasetKey)
          .put(EventSearchParameter.EVENT_ID_HIERARCHY, GbifTerm.datasetKey)
          .put(EventSearchParameter.EVENT_TYPE, GbifTerm.datasetKey)
          .put(EventSearchParameter.VERBATIM_EVENT_TYPE, GbifTerm.datasetKey)
          .put(EventSearchParameter.ISSUE, GbifTerm.datasetKey)
          .put(EventSearchParameter.HIGHER_GEOGRAPHY, GbifTerm.datasetKey)
          .put(EventSearchParameter.HIGHER_GEOGRAPHY, GbifTerm.datasetKey)

          // Humboldt
          .put(EventSearchParameter.HUMBOLDT_SITE_COUNT, EcoTerm.siteCount)
          .put(EventSearchParameter.HUMBOLDT_VERBATIM_SITE_NAMES, EcoTerm.verbatimSiteNames)
          .put(
              EventSearchParameter.HUMBOLDT_GEOSPATIAL_SCOPE_AREA_VALUE,
              EcoTerm.geospatialScopeAreaValue)
          .put(
              EventSearchParameter.HUMBOLDT_GEOSPATIAL_SCOPE_AREA_UNIT,
              EcoTerm.geospatialScopeAreaUnit)
          .put(
              EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_VALUE, EcoTerm.totalAreaSampledValue)
          .put(EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT, EcoTerm.totalAreaSampledUnit)
          .put(EventSearchParameter.HUMBOLDT_TARGET_HABITAT_SCOPE, EcoTerm.targetHabitatScope)
          .put(
              EventSearchParameter.HUMBOLDT_EVENT_DURATION,
              GbifInternalTerm.humboldtEventDurationValueInMinutes)
          .put(
              EventSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES,
              GbifInternalTerm.humboldtEventDurationValueInMinutes)
          .put(EventSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE, EcoTerm.eventDurationValue)
          .put(EventSearchParameter.HUMBOLDT_EVENT_DURATION_UNIT, EcoTerm.eventDurationUnit)
          .put(
              EventSearchParameter.HUMBOLDT_TAXON_COMPLETENESS_PROTOCOLS,
              EcoTerm.taxonCompletenessProtocols)
          .put(
              EventSearchParameter.HUMBOLDT_IS_TAXONOMIC_SCOPE_FULLY_REPORTED,
              EcoTerm.isTaxonomicScopeFullyReported)
          .put(EventSearchParameter.HUMBOLDT_IS_ABSENCE_REPORTED, EcoTerm.isAbsenceReported)
          .put(EventSearchParameter.HUMBOLDT_HAS_NON_TARGET_TAXA, EcoTerm.hasNonTargetTaxa)
          .put(
              EventSearchParameter.HUMBOLDT_ARE_NON_TARGET_TAXA_FULLY_REPORTED,
              EcoTerm.areNonTargetTaxaFullyReported)
          .put(EventSearchParameter.HUMBOLDT_TARGET_LIFE_STAGE_SCOPE, EcoTerm.targetLifeStageScope)
          .put(
              EventSearchParameter.HUMBOLDT_IS_LIFE_STAGE_SCOPE_FULLY_REPORTED,
              EcoTerm.isLifeStageScopeFullyReported)
          .put(
              EventSearchParameter.HUMBOLDT_TARGET_DEGREE_OF_ESTABLISHMENT_SCOPE,
              EcoTerm.targetDegreeOfEstablishmentScope)
          .put(
              EventSearchParameter.HUMBOLDT_IS_DEGREE_OF_ESTABLISHMENT_SCOPE_FULLY_REPORTED,
              EcoTerm.isDegreeOfEstablishmentScopeFullyReported)
          .put(
              EventSearchParameter.HUMBOLDT_TARGET_GROWTH_FORM_SCOPE, EcoTerm.targetGrowthFormScope)
          .put(
              EventSearchParameter.HUMBOLDT_IS_GROWTH_FORM_SCOPE_FULLY_REPORTED,
              EcoTerm.isGrowthFormScopeFullyReported)
          .put(
              EventSearchParameter.HUMBOLDT_HAS_NON_TARGET_ORGANISMS, EcoTerm.hasNonTargetOrganisms)
          .put(EventSearchParameter.HUMBOLDT_COMPILATION_TYPES, EcoTerm.compilationTypes)
          .put(
              EventSearchParameter.HUMBOLDT_COMPILATION_SOURCE_TYPES,
              EcoTerm.compilationSourceTypes)
          .put(EventSearchParameter.HUMBOLDT_INVENTORY_TYPES, EcoTerm.inventoryTypes)
          .put(EventSearchParameter.HUMBOLDT_PROTOCOL_NAMES, EcoTerm.protocolNames)
          .put(EventSearchParameter.HUMBOLDT_IS_ABUNDANCE_REPORTED, EcoTerm.isAbundanceReported)
          .put(
              EventSearchParameter.HUMBOLDT_IS_ABUNDANCE_CAP_REPORTED,
              EcoTerm.isAbundanceCapReported)
          .put(EventSearchParameter.HUMBOLDT_ABUNDANCE_CAP, EcoTerm.abundanceCap)
          .put(
              EventSearchParameter.HUMBOLDT_IS_VEGETATION_COVER_REPORTED,
              EcoTerm.isVegetationCoverReported)
          .put(
              EventSearchParameter.HUMBOLDT_IS_LEAST_SPECIFIC_TARGET_CATEGORY_QUANTITY_INCLUSIVE,
              EcoTerm.isLeastSpecificTargetCategoryQuantityInclusive)
          .put(EventSearchParameter.HUMBOLDT_HAS_VOUCHERS, EcoTerm.hasVouchers)
          .put(EventSearchParameter.HUMBOLDT_VOUCHER_INSTITUTIONS, EcoTerm.voucherInstitutions)
          .put(EventSearchParameter.HUMBOLDT_HAS_MATERIAL_SAMPLES, EcoTerm.hasMaterialSamples)
          .put(EventSearchParameter.HUMBOLDT_MATERIAL_SAMPLE_TYPES, EcoTerm.materialSampleTypes)
          .put(EventSearchParameter.HUMBOLDT_SAMPLING_PERFORMED_BY, EcoTerm.samplingPerformedBy)
          .put(
              EventSearchParameter.HUMBOLDT_IS_SAMPLING_EFFORT_REPORTED,
              EcoTerm.isSamplingEffortReported)
          .put(EventSearchParameter.HUMBOLDT_SAMPLING_EFFORT_VALUE, EcoTerm.samplingEffortValue)
          .put(EventSearchParameter.HUMBOLDT_SAMPLING_EFFORT_UNIT, EcoTerm.samplingEffortUnit)
          .build();

  // parameters that map directly to Hive.
  // boundingBox, coordinate, taxonKey and gadmGid are treated specially!
  private static final Map<? extends SearchParameter, ? extends Term> PARAM_TO_TERM =
      ImmutableMap.<SearchParameter, Term>builder()
          .put(OccurrenceSearchParameter.DATASET_KEY, GbifTerm.datasetKey)
          .put(OccurrenceSearchParameter.SEX, DwcTerm.sex)
          .put(OccurrenceSearchParameter.YEAR, DwcTerm.year)
          .put(OccurrenceSearchParameter.MONTH, DwcTerm.month)
          .put(OccurrenceSearchParameter.DAY, DwcTerm.day)
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
          .put(OccurrenceSearchParameter.START_DAY_OF_YEAR, DwcTerm.startDayOfYear)
          .put(OccurrenceSearchParameter.END_DAY_OF_YEAR, DwcTerm.endDayOfYear)
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
          .put(OccurrenceSearchParameter.TAXONOMIC_ISSUE, GbifTerm.taxonomicIssue)
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
          .put(OccurrenceSearchParameter.PROGRAMME, GbifInternalTerm.programmeAcronym)
          .put(OccurrenceSearchParameter.VERBATIM_SCIENTIFIC_NAME, GbifTerm.verbatimScientificName)
          .put(OccurrenceSearchParameter.TAXON_ID, DwcTerm.taxonID)
          .put(OccurrenceSearchParameter.TAXON_CONCEPT_ID, DwcTerm.taxonConceptID)
          .put(OccurrenceSearchParameter.SAMPLE_SIZE_UNIT, DwcTerm.sampleSizeUnit)
          .put(OccurrenceSearchParameter.SAMPLE_SIZE_VALUE, DwcTerm.sampleSizeValue)
          .put(OccurrenceSearchParameter.PREVIOUS_IDENTIFICATIONS, DwcTerm.previousIdentifications)
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
          .put(OccurrenceSearchParameter.GBIF_ID, GbifTerm.gbifID)
          .put(InternalOccurrenceSearchParameter.EVENT_DATE_GTE, GbifInternalTerm.eventDateGte)
          .put(InternalOccurrenceSearchParameter.EVENT_DATE_LTE, GbifInternalTerm.eventDateLte)
          .put(OccurrenceSearchParameter.GBIF_REGION, GbifTerm.gbifRegion)
          .put(OccurrenceSearchParameter.PUBLISHED_BY_GBIF_REGION, GbifTerm.publishedByGbifRegion)
          .put(OccurrenceSearchParameter.IS_SEQUENCED, GbifTerm.isSequenced)
          .put(OccurrenceSearchParameter.ISLAND, DwcTerm.island)
          .put(OccurrenceSearchParameter.ISLAND_GROUP, DwcTerm.islandGroup)
          .put(OccurrenceSearchParameter.FIELD_NUMBER, DwcTerm.fieldNumber)
          .put(
              OccurrenceSearchParameter.EARLIEST_EON_OR_LOWEST_EONOTHEM,
              DwcTerm.earliestEonOrLowestEonothem)
          .put(
              OccurrenceSearchParameter.LATEST_EON_OR_HIGHEST_EONOTHEM,
              DwcTerm.latestEonOrHighestEonothem)
          .put(
              OccurrenceSearchParameter.EARLIEST_ERA_OR_LOWEST_ERATHEM,
              DwcTerm.earliestEraOrLowestErathem)
          .put(
              OccurrenceSearchParameter.LATEST_ERA_OR_HIGHEST_ERATHEM,
              DwcTerm.latestEraOrHighestErathem)
          .put(
              OccurrenceSearchParameter.EARLIEST_PERIOD_OR_LOWEST_SYSTEM,
              DwcTerm.earliestPeriodOrLowestSystem)
          .put(
              OccurrenceSearchParameter.LATEST_PERIOD_OR_HIGHEST_SYSTEM,
              DwcTerm.latestPeriodOrHighestSystem)
          .put(
              OccurrenceSearchParameter.EARLIEST_EPOCH_OR_LOWEST_SERIES,
              DwcTerm.earliestEpochOrLowestSeries)
          .put(
              OccurrenceSearchParameter.LATEST_EPOCH_OR_HIGHEST_SERIES,
              DwcTerm.latestEpochOrHighestSeries)
          .put(
              OccurrenceSearchParameter.EARLIEST_AGE_OR_LOWEST_STAGE,
              DwcTerm.earliestAgeOrLowestStage)
          .put(
              OccurrenceSearchParameter.LATEST_AGE_OR_HIGHEST_STAGE,
              DwcTerm.latestAgeOrHighestStage)
          .put(
              OccurrenceSearchParameter.LOWEST_BIOSTRATIGRAPHIC_ZONE,
              DwcTerm.lowestBiostratigraphicZone)
          .put(
              OccurrenceSearchParameter.HIGHEST_BIOSTRATIGRAPHIC_ZONE,
              DwcTerm.highestBiostratigraphicZone)
          .put(OccurrenceSearchParameter.GROUP, DwcTerm.group)
          .put(OccurrenceSearchParameter.FORMATION, DwcTerm.formation)
          .put(OccurrenceSearchParameter.MEMBER, DwcTerm.member)
          .put(OccurrenceSearchParameter.BED, DwcTerm.bed)
          .put(OccurrenceSearchParameter.PROJECT_ID, GbifTerm.projectId)
          .put(OccurrenceSearchParameter.CHECKLIST_KEY, GbifTerm.checklistKey)
          .put(OccurrenceSearchParameter.GEOLOGICAL_TIME, GbifTerm.geologicalTime)
          .put(OccurrenceSearchParameter.LITHOSTRATIGRAPHY, GbifTerm.lithostratigraphy)
          .put(OccurrenceSearchParameter.BIOSTRATIGRAPHY, GbifTerm.biostratigraphy)
          .put(OccurrenceSearchParameter.EVENT_DATE_GTE, GbifInternalTerm.eventDateGte)
          .put(OccurrenceSearchParameter.DNA_SEQUENCE_ID, GbifTerm.dnaSequenceID)
          .putAll(EVENTS_PARAM_TO_TERM)
          .build();

  private static final Map<? extends SearchParameter, Term> ARRAY_STRING_TERMS =
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
          .put(OccurrenceSearchParameter.PROJECT_ID, GbifTerm.projectId)
          .put(OccurrenceSearchParameter.GEOREFERENCED_BY, DwcTerm.georeferencedBy)
          .put(OccurrenceSearchParameter.HIGHER_GEOGRAPHY, DwcTerm.higherGeography)
          .put(OccurrenceSearchParameter.ASSOCIATED_SEQUENCES, DwcTerm.associatedSequences)
          .put(OccurrenceSearchParameter.DNA_SEQUENCE_ID, GbifTerm.dnaSequenceID)
          .build();

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
