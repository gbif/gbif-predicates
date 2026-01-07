package org.gbif.predicate.query.occurrence;

import java.util.Collections;
import java.util.LinkedHashMap;
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

  static Map<? extends SearchParameter, ? extends Term> EVENTS_PARAM_TO_TERM;
  private static final Map<? extends SearchParameter, ? extends Term> PARAM_TO_TERM;
  private static final Map<? extends SearchParameter, Term> ARRAY_STRING_TERMS;

  static {
    // Build EVENTS_PARAM_TO_TERM with LinkedHashMap to preserve insertion order
    Map<SearchParameter, Term> eventsMap = new LinkedHashMap<>();
    eventsMap.put(EventSearchParameter.DATASET_KEY, GbifTerm.datasetKey);
    eventsMap.put(EventSearchParameter.CHECKLIST_KEY, GbifTerm.checklistKey);
    eventsMap.put(EventSearchParameter.YEAR, DwcTerm.year);
    eventsMap.put(EventSearchParameter.MONTH, DwcTerm.month);
    eventsMap.put(EventSearchParameter.DAY, DwcTerm.day);
    eventsMap.put(EventSearchParameter.START_DAY_OF_YEAR, DwcTerm.startDayOfYear);
    eventsMap.put(EventSearchParameter.END_DAY_OF_YEAR, DwcTerm.endDayOfYear);
    eventsMap.put(EventSearchParameter.EVENT_DATE, DwcTerm.eventDate);
    eventsMap.put(EventSearchParameter.EVENT_DATE_GTE, GbifInternalTerm.eventDateGte);
    eventsMap.put(EventSearchParameter.EVENT_ID, DwcTerm.eventID);
    eventsMap.put(EventSearchParameter.PARENT_EVENT_ID, DwcTerm.parentEventID);
    eventsMap.put(EventSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol);
    eventsMap.put(EventSearchParameter.PREVIOUS_IDENTIFICATIONS, DwcTerm.previousIdentifications);
    eventsMap.put(EventSearchParameter.LAST_INTERPRETED, GbifTerm.lastInterpreted);
    eventsMap.put(EventSearchParameter.MODIFIED, DcTerm.modified);
    eventsMap.put(EventSearchParameter.DECIMAL_LATITUDE, DwcTerm.decimalLatitude);
    eventsMap.put(EventSearchParameter.DECIMAL_LONGITUDE, DwcTerm.decimalLongitude);
    eventsMap.put(
        EventSearchParameter.COORDINATE_UNCERTAINTY_IN_METERS,
        DwcTerm.coordinateUncertaintyInMeters);
    eventsMap.put(EventSearchParameter.COUNTRY, DwcTerm.country);
    eventsMap.put(EventSearchParameter.GBIF_REGION, GbifTerm.gbifRegion);
    eventsMap.put(EventSearchParameter.CONTINENT, DwcTerm.continent);
    eventsMap.put(EventSearchParameter.PUBLISHING_COUNTRY, GbifTerm.publishingCountry);
    eventsMap.put(EventSearchParameter.PUBLISHED_BY_GBIF_REGION, GbifTerm.publishedByGbifRegion);
    eventsMap.put(EventSearchParameter.ELEVATION, GbifTerm.elevation);
    eventsMap.put(EventSearchParameter.DEPTH, GbifTerm.depth);
    eventsMap.put(EventSearchParameter.INSTITUTION_CODE, DwcTerm.institutionCode);
    eventsMap.put(EventSearchParameter.COLLECTION_CODE, DwcTerm.collectionCode);
    eventsMap.put(EventSearchParameter.TAXON_KEY, GbifTerm.taxonKey);
    eventsMap.put(EventSearchParameter.ACCEPTED_TAXON_KEY, GbifTerm.acceptedTaxonKey);
    eventsMap.put(EventSearchParameter.SCIENTIFIC_NAME, DwcTerm.scientificName);
    eventsMap.put(EventSearchParameter.TAXON_ID, DwcTerm.taxonID);
    eventsMap.put(EventSearchParameter.TAXONOMIC_STATUS, DwcTerm.taxonomicStatus);
    eventsMap.put(EventSearchParameter.TAXONOMIC_ISSUE, GbifTerm.taxonomicIssue);
    eventsMap.put(EventSearchParameter.IUCN_RED_LIST_CATEGORY, IucnTerm.iucnRedListCategory);
    eventsMap.put(EventSearchParameter.HAS_COORDINATE, GbifTerm.hasCoordinate);
    eventsMap.put(EventSearchParameter.HAS_GEOSPATIAL_ISSUE, GbifTerm.hasGeospatialIssues);
    eventsMap.put(EventSearchParameter.MEDIA_TYPE, GbifTerm.mediaType);
    eventsMap.put(EventSearchParameter.REPATRIATED, GbifTerm.repatriated);
    eventsMap.put(EventSearchParameter.STATE_PROVINCE, DwcTerm.stateProvince);
    eventsMap.put(EventSearchParameter.WATER_BODY, DwcTerm.waterBody);
    eventsMap.put(EventSearchParameter.LOCALITY, DwcTerm.locality);
    eventsMap.put(EventSearchParameter.PROTOCOL, GbifTerm.protocol);
    eventsMap.put(EventSearchParameter.LICENSE, DcTerm.license);
    eventsMap.put(EventSearchParameter.PUBLISHING_ORG, GbifInternalTerm.publishingOrgKey);
    eventsMap.put(EventSearchParameter.NETWORK_KEY, GbifInternalTerm.networkKey);
    eventsMap.put(EventSearchParameter.INSTALLATION_KEY, GbifInternalTerm.installationKey);
    eventsMap.put(
        EventSearchParameter.HOSTING_ORGANIZATION_KEY, GbifInternalTerm.hostingOrganizationKey);
    eventsMap.put(EventSearchParameter.CRAWL_ID, GbifInternalTerm.crawlId);
    eventsMap.put(EventSearchParameter.PROJECT_ID, GbifTerm.projectId);
    eventsMap.put(EventSearchParameter.PROGRAMME, GbifInternalTerm.programmeAcronym);
    eventsMap.put(EventSearchParameter.SAMPLE_SIZE_UNIT, DwcTerm.sampleSizeUnit);
    eventsMap.put(EventSearchParameter.SAMPLE_SIZE_VALUE, DwcTerm.sampleSizeValue);
    eventsMap.put(EventSearchParameter.GADM_LEVEL_0_GID, GadmTerm.level0Gid);
    eventsMap.put(EventSearchParameter.GADM_LEVEL_1_GID, GadmTerm.level1Gid);
    eventsMap.put(EventSearchParameter.GADM_LEVEL_2_GID, GadmTerm.level2Gid);
    eventsMap.put(EventSearchParameter.GADM_LEVEL_3_GID, GadmTerm.level3Gid);
    eventsMap.put(EventSearchParameter.DWCA_EXTENSION, GbifInternalTerm.dwcaExtension);
    eventsMap.put(EventSearchParameter.DATASET_ID, DwcTerm.datasetID);
    eventsMap.put(EventSearchParameter.DATASET_NAME, DwcTerm.datasetName);
    eventsMap.put(EventSearchParameter.ISLAND, DwcTerm.island);
    eventsMap.put(EventSearchParameter.ISLAND_GROUP, DwcTerm.islandGroup);
    eventsMap.put(EventSearchParameter.GEOREFERENCED_BY, DwcTerm.georeferencedBy);
    eventsMap.put(EventSearchParameter.HIGHER_GEOGRAPHY, DwcTerm.higherGeography);
    eventsMap.put(EventSearchParameter.FIELD_NUMBER, DwcTerm.fieldNumber);
    eventsMap.put(EventSearchParameter.GBIF_ID, GbifTerm.gbifID);
    eventsMap.put(EventSearchParameter.EVENT_TYPE, DwcTerm.eventType);
    eventsMap.put(EventSearchParameter.ISSUE, GbifTerm.issue);
    // Humboldt
    eventsMap.put(EventSearchParameter.HUMBOLDT_SITE_COUNT, EcoTerm.siteCount);
    eventsMap.put(EventSearchParameter.HUMBOLDT_VERBATIM_SITE_NAMES, EcoTerm.verbatimSiteNames);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_GEOSPATIAL_SCOPE_AREA_VALUE,
        EcoTerm.geospatialScopeAreaValue);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_GEOSPATIAL_SCOPE_AREA_UNIT, EcoTerm.geospatialScopeAreaUnit);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_VALUE, EcoTerm.totalAreaSampledValue);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT, EcoTerm.totalAreaSampledUnit);
    eventsMap.put(EventSearchParameter.HUMBOLDT_TARGET_HABITAT_SCOPE, EcoTerm.targetHabitatScope);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_EVENT_DURATION,
        GbifInternalTerm.humboldtEventDurationValueInMinutes);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES,
        GbifInternalTerm.humboldtEventDurationValueInMinutes);
    eventsMap.put(EventSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE, EcoTerm.eventDurationValue);
    eventsMap.put(EventSearchParameter.HUMBOLDT_EVENT_DURATION_UNIT, EcoTerm.eventDurationUnit);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_TAXON_COMPLETENESS_PROTOCOLS,
        EcoTerm.taxonCompletenessProtocols);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_IS_TAXONOMIC_SCOPE_FULLY_REPORTED,
        EcoTerm.isTaxonomicScopeFullyReported);
    eventsMap.put(EventSearchParameter.HUMBOLDT_IS_ABSENCE_REPORTED, EcoTerm.isAbsenceReported);
    eventsMap.put(EventSearchParameter.HUMBOLDT_HAS_NON_TARGET_TAXA, EcoTerm.hasNonTargetTaxa);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_ARE_NON_TARGET_TAXA_FULLY_REPORTED,
        EcoTerm.areNonTargetTaxaFullyReported);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_TARGET_LIFE_STAGE_SCOPE, EcoTerm.targetLifeStageScope);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_IS_LIFE_STAGE_SCOPE_FULLY_REPORTED,
        EcoTerm.isLifeStageScopeFullyReported);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_TARGET_DEGREE_OF_ESTABLISHMENT_SCOPE,
        EcoTerm.targetDegreeOfEstablishmentScope);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_IS_DEGREE_OF_ESTABLISHMENT_SCOPE_FULLY_REPORTED,
        EcoTerm.isDegreeOfEstablishmentScopeFullyReported);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_TARGET_GROWTH_FORM_SCOPE, EcoTerm.targetGrowthFormScope);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_IS_GROWTH_FORM_SCOPE_FULLY_REPORTED,
        EcoTerm.isGrowthFormScopeFullyReported);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_HAS_NON_TARGET_ORGANISMS, EcoTerm.hasNonTargetOrganisms);
    eventsMap.put(EventSearchParameter.HUMBOLDT_COMPILATION_TYPES, EcoTerm.compilationTypes);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_COMPILATION_SOURCE_TYPES, EcoTerm.compilationSourceTypes);
    eventsMap.put(EventSearchParameter.HUMBOLDT_INVENTORY_TYPES, EcoTerm.inventoryTypes);
    eventsMap.put(EventSearchParameter.HUMBOLDT_PROTOCOL_NAMES, EcoTerm.protocolNames);
    eventsMap.put(EventSearchParameter.HUMBOLDT_IS_ABUNDANCE_REPORTED, EcoTerm.isAbundanceReported);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_IS_ABUNDANCE_CAP_REPORTED, EcoTerm.isAbundanceCapReported);
    eventsMap.put(EventSearchParameter.HUMBOLDT_ABUNDANCE_CAP, EcoTerm.abundanceCap);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_IS_VEGETATION_COVER_REPORTED,
        EcoTerm.isVegetationCoverReported);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_IS_LEAST_SPECIFIC_TARGET_CATEGORY_QUANTITY_INCLUSIVE,
        EcoTerm.isLeastSpecificTargetCategoryQuantityInclusive);
    eventsMap.put(EventSearchParameter.HUMBOLDT_HAS_VOUCHERS, EcoTerm.hasVouchers);
    eventsMap.put(EventSearchParameter.HUMBOLDT_VOUCHER_INSTITUTIONS, EcoTerm.voucherInstitutions);
    eventsMap.put(EventSearchParameter.HUMBOLDT_HAS_MATERIAL_SAMPLES, EcoTerm.hasMaterialSamples);
    eventsMap.put(EventSearchParameter.HUMBOLDT_MATERIAL_SAMPLE_TYPES, EcoTerm.materialSampleTypes);
    eventsMap.put(EventSearchParameter.HUMBOLDT_SAMPLING_PERFORMED_BY, EcoTerm.samplingPerformedBy);
    eventsMap.put(
        EventSearchParameter.HUMBOLDT_IS_SAMPLING_EFFORT_REPORTED,
        EcoTerm.isSamplingEffortReported);
    eventsMap.put(EventSearchParameter.HUMBOLDT_SAMPLING_EFFORT_VALUE, EcoTerm.samplingEffortValue);
    eventsMap.put(EventSearchParameter.HUMBOLDT_SAMPLING_EFFORT_UNIT, EcoTerm.samplingEffortUnit);
    EVENTS_PARAM_TO_TERM = Collections.unmodifiableMap(eventsMap);

    // Build PARAM_TO_TERM with LinkedHashMap to preserve insertion order
    // parameters that map directly to Hive.
    // boundingBox, coordinate, taxonKey and gadmGid are treated specially!
    Map<SearchParameter, Term> paramMap = new LinkedHashMap<>();
    paramMap.put(OccurrenceSearchParameter.DATASET_KEY, GbifTerm.datasetKey);
    paramMap.put(OccurrenceSearchParameter.SEX, DwcTerm.sex);
    paramMap.put(OccurrenceSearchParameter.YEAR, DwcTerm.year);
    paramMap.put(OccurrenceSearchParameter.MONTH, DwcTerm.month);
    paramMap.put(OccurrenceSearchParameter.DAY, DwcTerm.day);
    paramMap.put(OccurrenceSearchParameter.DECIMAL_LATITUDE, DwcTerm.decimalLatitude);
    paramMap.put(OccurrenceSearchParameter.DECIMAL_LONGITUDE, DwcTerm.decimalLongitude);
    paramMap.put(OccurrenceSearchParameter.ELEVATION, GbifTerm.elevation);
    paramMap.put(OccurrenceSearchParameter.DEPTH, GbifTerm.depth);
    paramMap.put(OccurrenceSearchParameter.INSTITUTION_CODE, DwcTerm.institutionCode);
    paramMap.put(OccurrenceSearchParameter.COLLECTION_CODE, DwcTerm.collectionCode);
    paramMap.put(OccurrenceSearchParameter.CATALOG_NUMBER, DwcTerm.catalogNumber);
    paramMap.put(OccurrenceSearchParameter.SCIENTIFIC_NAME, DwcTerm.scientificName);
    paramMap.put(OccurrenceSearchParameter.OCCURRENCE_ID, DwcTerm.occurrenceID);
    paramMap.put(OccurrenceSearchParameter.ESTABLISHMENT_MEANS, DwcTerm.establishmentMeans);
    paramMap.put(OccurrenceSearchParameter.DEGREE_OF_ESTABLISHMENT, DwcTerm.degreeOfEstablishment);
    paramMap.put(OccurrenceSearchParameter.PATHWAY, DwcTerm.pathway);
    // the following need some value transformation
    paramMap.put(OccurrenceSearchParameter.EVENT_DATE, DwcTerm.eventDate);
    paramMap.put(OccurrenceSearchParameter.START_DAY_OF_YEAR, DwcTerm.startDayOfYear);
    paramMap.put(OccurrenceSearchParameter.END_DAY_OF_YEAR, DwcTerm.endDayOfYear);
    paramMap.put(OccurrenceSearchParameter.MODIFIED, DcTerm.modified);
    paramMap.put(OccurrenceSearchParameter.LAST_INTERPRETED, GbifTerm.lastInterpreted);
    paramMap.put(OccurrenceSearchParameter.BASIS_OF_RECORD, DwcTerm.basisOfRecord);
    paramMap.put(OccurrenceSearchParameter.COUNTRY, DwcTerm.countryCode);
    paramMap.put(OccurrenceSearchParameter.CONTINENT, DwcTerm.continent);
    paramMap.put(OccurrenceSearchParameter.PUBLISHING_COUNTRY, GbifTerm.publishingCountry);
    paramMap.put(OccurrenceSearchParameter.RECORDED_BY, DwcTerm.recordedBy);
    paramMap.put(OccurrenceSearchParameter.IDENTIFIED_BY, DwcTerm.identifiedBy);
    paramMap.put(OccurrenceSearchParameter.RECORD_NUMBER, DwcTerm.recordNumber);
    paramMap.put(OccurrenceSearchParameter.TYPE_STATUS, DwcTerm.typeStatus);
    paramMap.put(OccurrenceSearchParameter.HAS_COORDINATE, GbifTerm.hasCoordinate);
    paramMap.put(OccurrenceSearchParameter.HAS_GEOSPATIAL_ISSUE, GbifTerm.hasGeospatialIssues);
    paramMap.put(OccurrenceSearchParameter.MEDIA_TYPE, GbifTerm.mediaType);
    paramMap.put(OccurrenceSearchParameter.ISSUE, GbifTerm.issue);
    paramMap.put(OccurrenceSearchParameter.TAXONOMIC_ISSUE, GbifTerm.taxonomicIssue);
    paramMap.put(OccurrenceSearchParameter.TAXON_KEY, GbifTerm.taxonKey);
    paramMap.put(OccurrenceSearchParameter.KINGDOM_KEY, GbifTerm.kingdomKey);
    paramMap.put(OccurrenceSearchParameter.PHYLUM_KEY, GbifTerm.phylumKey);
    paramMap.put(OccurrenceSearchParameter.CLASS_KEY, GbifTerm.classKey);
    paramMap.put(OccurrenceSearchParameter.ORDER_KEY, GbifTerm.orderKey);
    paramMap.put(OccurrenceSearchParameter.FAMILY_KEY, GbifTerm.familyKey);
    paramMap.put(OccurrenceSearchParameter.GENUS_KEY, GbifTerm.genusKey);
    paramMap.put(OccurrenceSearchParameter.SUBGENUS_KEY, GbifTerm.subgenusKey);
    paramMap.put(OccurrenceSearchParameter.SPECIES_KEY, GbifTerm.speciesKey);
    paramMap.put(OccurrenceSearchParameter.ACCEPTED_TAXON_KEY, GbifTerm.acceptedTaxonKey);
    paramMap.put(OccurrenceSearchParameter.TAXONOMIC_STATUS, DwcTerm.taxonomicStatus);
    paramMap.put(OccurrenceSearchParameter.REPATRIATED, GbifTerm.repatriated);
    paramMap.put(OccurrenceSearchParameter.ORGANISM_ID, DwcTerm.organismID);
    paramMap.put(OccurrenceSearchParameter.LOCALITY, DwcTerm.locality);
    paramMap.put(
        OccurrenceSearchParameter.COORDINATE_UNCERTAINTY_IN_METERS,
        DwcTerm.coordinateUncertaintyInMeters);
    paramMap.put(OccurrenceSearchParameter.STATE_PROVINCE, DwcTerm.stateProvince);
    paramMap.put(OccurrenceSearchParameter.WATER_BODY, DwcTerm.waterBody);
    paramMap.put(OccurrenceSearchParameter.GADM_LEVEL_0_GID, GadmTerm.level0Gid);
    paramMap.put(OccurrenceSearchParameter.GADM_LEVEL_1_GID, GadmTerm.level1Gid);
    paramMap.put(OccurrenceSearchParameter.GADM_LEVEL_2_GID, GadmTerm.level2Gid);
    paramMap.put(OccurrenceSearchParameter.GADM_LEVEL_3_GID, GadmTerm.level3Gid);
    paramMap.put(OccurrenceSearchParameter.PROTOCOL, GbifTerm.protocol);
    paramMap.put(OccurrenceSearchParameter.LICENSE, DcTerm.license);
    paramMap.put(OccurrenceSearchParameter.PUBLISHING_ORG, GbifInternalTerm.publishingOrgKey);
    paramMap.put(
        OccurrenceSearchParameter.HOSTING_ORGANIZATION_KEY,
        GbifInternalTerm.hostingOrganizationKey);
    paramMap.put(OccurrenceSearchParameter.CRAWL_ID, GbifInternalTerm.crawlId);
    paramMap.put(OccurrenceSearchParameter.INSTALLATION_KEY, GbifInternalTerm.installationKey);
    paramMap.put(OccurrenceSearchParameter.NETWORK_KEY, GbifInternalTerm.networkKey);
    paramMap.put(OccurrenceSearchParameter.EVENT_ID, DwcTerm.eventID);
    paramMap.put(OccurrenceSearchParameter.PARENT_EVENT_ID, DwcTerm.parentEventID);
    paramMap.put(OccurrenceSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol);
    paramMap.put(OccurrenceSearchParameter.PROGRAMME, GbifInternalTerm.programmeAcronym);
    paramMap.put(
        OccurrenceSearchParameter.VERBATIM_SCIENTIFIC_NAME, GbifTerm.verbatimScientificName);
    paramMap.put(OccurrenceSearchParameter.TAXON_ID, DwcTerm.taxonID);
    paramMap.put(OccurrenceSearchParameter.TAXON_CONCEPT_ID, DwcTerm.taxonConceptID);
    paramMap.put(OccurrenceSearchParameter.SAMPLE_SIZE_UNIT, DwcTerm.sampleSizeUnit);
    paramMap.put(OccurrenceSearchParameter.SAMPLE_SIZE_VALUE, DwcTerm.sampleSizeValue);
    paramMap.put(
        OccurrenceSearchParameter.PREVIOUS_IDENTIFICATIONS, DwcTerm.previousIdentifications);
    paramMap.put(OccurrenceSearchParameter.ORGANISM_QUANTITY, DwcTerm.organismQuantity);
    paramMap.put(OccurrenceSearchParameter.ORGANISM_QUANTITY_TYPE, DwcTerm.organismQuantityType);
    paramMap.put(
        OccurrenceSearchParameter.RELATIVE_ORGANISM_QUANTITY, GbifTerm.relativeOrganismQuantity);
    paramMap.put(OccurrenceSearchParameter.COLLECTION_KEY, GbifInternalTerm.collectionKey);
    paramMap.put(OccurrenceSearchParameter.INSTITUTION_KEY, GbifInternalTerm.institutionKey);
    paramMap.put(OccurrenceSearchParameter.RECORDED_BY_ID, DwcTerm.recordedByID);
    paramMap.put(OccurrenceSearchParameter.IDENTIFIED_BY_ID, DwcTerm.identifiedByID);
    paramMap.put(OccurrenceSearchParameter.OCCURRENCE_STATUS, DwcTerm.occurrenceStatus);
    paramMap.put(OccurrenceSearchParameter.LIFE_STAGE, DwcTerm.lifeStage);
    paramMap.put(OccurrenceSearchParameter.IS_IN_CLUSTER, GbifInternalTerm.isInCluster);
    paramMap.put(OccurrenceSearchParameter.DWCA_EXTENSION, GbifInternalTerm.dwcaExtension);
    paramMap.put(OccurrenceSearchParameter.IUCN_RED_LIST_CATEGORY, IucnTerm.iucnRedListCategory);
    paramMap.put(OccurrenceSearchParameter.DATASET_ID, DwcTerm.datasetID);
    paramMap.put(OccurrenceSearchParameter.DATASET_NAME, DwcTerm.datasetName);
    paramMap.put(OccurrenceSearchParameter.OTHER_CATALOG_NUMBERS, DwcTerm.otherCatalogNumbers);
    paramMap.put(OccurrenceSearchParameter.PREPARATIONS, DwcTerm.preparations);
    paramMap.put(
        OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS,
        GbifTerm.distanceFromCentroidInMeters);
    paramMap.put(OccurrenceSearchParameter.GBIF_ID, GbifTerm.gbifID);
    paramMap.put(InternalOccurrenceSearchParameter.EVENT_DATE_GTE, GbifInternalTerm.eventDateGte);
    paramMap.put(InternalOccurrenceSearchParameter.EVENT_DATE_LTE, GbifInternalTerm.eventDateLte);
    paramMap.put(OccurrenceSearchParameter.GBIF_REGION, GbifTerm.gbifRegion);
    paramMap.put(
        OccurrenceSearchParameter.PUBLISHED_BY_GBIF_REGION, GbifTerm.publishedByGbifRegion);
    paramMap.put(OccurrenceSearchParameter.IS_SEQUENCED, GbifTerm.isSequenced);
    paramMap.put(OccurrenceSearchParameter.ISLAND, DwcTerm.island);
    paramMap.put(OccurrenceSearchParameter.ISLAND_GROUP, DwcTerm.islandGroup);
    paramMap.put(OccurrenceSearchParameter.FIELD_NUMBER, DwcTerm.fieldNumber);
    paramMap.put(
        OccurrenceSearchParameter.EARLIEST_EON_OR_LOWEST_EONOTHEM,
        DwcTerm.earliestEonOrLowestEonothem);
    paramMap.put(
        OccurrenceSearchParameter.LATEST_EON_OR_HIGHEST_EONOTHEM,
        DwcTerm.latestEonOrHighestEonothem);
    paramMap.put(
        OccurrenceSearchParameter.EARLIEST_ERA_OR_LOWEST_ERATHEM,
        DwcTerm.earliestEraOrLowestErathem);
    paramMap.put(
        OccurrenceSearchParameter.LATEST_ERA_OR_HIGHEST_ERATHEM, DwcTerm.latestEraOrHighestErathem);
    paramMap.put(
        OccurrenceSearchParameter.EARLIEST_PERIOD_OR_LOWEST_SYSTEM,
        DwcTerm.earliestPeriodOrLowestSystem);
    paramMap.put(
        OccurrenceSearchParameter.LATEST_PERIOD_OR_HIGHEST_SYSTEM,
        DwcTerm.latestPeriodOrHighestSystem);
    paramMap.put(
        OccurrenceSearchParameter.EARLIEST_EPOCH_OR_LOWEST_SERIES,
        DwcTerm.earliestEpochOrLowestSeries);
    paramMap.put(
        OccurrenceSearchParameter.LATEST_EPOCH_OR_HIGHEST_SERIES,
        DwcTerm.latestEpochOrHighestSeries);
    paramMap.put(
        OccurrenceSearchParameter.EARLIEST_AGE_OR_LOWEST_STAGE, DwcTerm.earliestAgeOrLowestStage);
    paramMap.put(
        OccurrenceSearchParameter.LATEST_AGE_OR_HIGHEST_STAGE, DwcTerm.latestAgeOrHighestStage);
    paramMap.put(
        OccurrenceSearchParameter.LOWEST_BIOSTRATIGRAPHIC_ZONE, DwcTerm.lowestBiostratigraphicZone);
    paramMap.put(
        OccurrenceSearchParameter.HIGHEST_BIOSTRATIGRAPHIC_ZONE,
        DwcTerm.highestBiostratigraphicZone);
    paramMap.put(OccurrenceSearchParameter.GROUP, DwcTerm.group);
    paramMap.put(OccurrenceSearchParameter.FORMATION, DwcTerm.formation);
    paramMap.put(OccurrenceSearchParameter.MEMBER, DwcTerm.member);
    paramMap.put(OccurrenceSearchParameter.BED, DwcTerm.bed);
    paramMap.put(OccurrenceSearchParameter.PROJECT_ID, GbifTerm.projectId);
    paramMap.put(OccurrenceSearchParameter.CHECKLIST_KEY, GbifTerm.checklistKey);
    paramMap.put(OccurrenceSearchParameter.GEOLOGICAL_TIME, GbifTerm.geologicalTime);
    paramMap.put(OccurrenceSearchParameter.LITHOSTRATIGRAPHY, GbifTerm.lithostratigraphy);
    paramMap.put(OccurrenceSearchParameter.BIOSTRATIGRAPHY, GbifTerm.biostratigraphy);
    paramMap.put(OccurrenceSearchParameter.EVENT_DATE_GTE, GbifInternalTerm.eventDateGte);
    paramMap.put(OccurrenceSearchParameter.DNA_SEQUENCE_ID, GbifTerm.dnaSequenceID);
    // Add all from EVENTS_PARAM_TO_TERM
    paramMap.putAll(EVENTS_PARAM_TO_TERM);
    PARAM_TO_TERM = Collections.unmodifiableMap(paramMap);

    // Build ARRAY_STRING_TERMS with LinkedHashMap to preserve insertion order
    Map<SearchParameter, Term> arrayMap = new LinkedHashMap<>();
    arrayMap.put(OccurrenceSearchParameter.NETWORK_KEY, GbifInternalTerm.networkKey);
    arrayMap.put(OccurrenceSearchParameter.DWCA_EXTENSION, GbifInternalTerm.dwcaExtension);
    arrayMap.put(OccurrenceSearchParameter.IDENTIFIED_BY_ID, DwcTerm.identifiedByID);
    arrayMap.put(OccurrenceSearchParameter.RECORDED_BY_ID, DwcTerm.recordedByID);
    arrayMap.put(OccurrenceSearchParameter.DATASET_ID, DwcTerm.datasetID);
    arrayMap.put(OccurrenceSearchParameter.DATASET_NAME, DwcTerm.datasetName);
    arrayMap.put(OccurrenceSearchParameter.OTHER_CATALOG_NUMBERS, DwcTerm.otherCatalogNumbers);
    arrayMap.put(OccurrenceSearchParameter.RECORDED_BY, DwcTerm.recordedBy);
    arrayMap.put(OccurrenceSearchParameter.IDENTIFIED_BY, DwcTerm.identifiedBy);
    arrayMap.put(OccurrenceSearchParameter.PREPARATIONS, DwcTerm.preparations);
    arrayMap.put(OccurrenceSearchParameter.SAMPLING_PROTOCOL, DwcTerm.samplingProtocol);
    arrayMap.put(OccurrenceSearchParameter.PROJECT_ID, GbifTerm.projectId);
    arrayMap.put(OccurrenceSearchParameter.GEOREFERENCED_BY, DwcTerm.georeferencedBy);
    arrayMap.put(OccurrenceSearchParameter.HIGHER_GEOGRAPHY, DwcTerm.higherGeography);
    arrayMap.put(OccurrenceSearchParameter.ASSOCIATED_SEQUENCES, DwcTerm.associatedSequences);
    arrayMap.put(OccurrenceSearchParameter.DNA_SEQUENCE_ID, GbifTerm.dnaSequenceID);
    ARRAY_STRING_TERMS = Collections.unmodifiableMap(arrayMap);
  }

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
