package org.gbif.predicate.query.occurrence;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.gbif.api.model.common.search.SearchParameter;
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

          // Humboldt
        .put(OccurrenceSearchParameter.HUMBOLDT_SITE_COUNT, EcoTerm.siteCount)
        .put(OccurrenceSearchParameter.HUMBOLDT_VERBATIM_SITE_NAMES, EcoTerm.verbatimSiteNames)
        .put(OccurrenceSearchParameter.HUMBOLDT_GEOSPATIAL_SCOPE_AREA_VALUE, EcoTerm.geospatialScopeAreaValue)
        .put(OccurrenceSearchParameter.HUMBOLDT_GEOSPATIAL_SCOPE_AREA_UNIT, EcoTerm.geospatialScopeAreaUnit)
        .put(OccurrenceSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_VALUE, EcoTerm.totalAreaSampledValue)
        .put(OccurrenceSearchParameter.HUMBOLDT_TOTAL_AREA_SAMPLED_UNIT, EcoTerm.totalAreaSampledUnit)
        .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_HABITAT_SCOPE, EcoTerm.targetHabitatScope)
        .put(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE_IN_MINUTES, GbifInternalTerm.humboldtEventDurationValueInMinutes)
        .put(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_VALUE, EcoTerm.eventDurationValue)
        .put(OccurrenceSearchParameter.HUMBOLDT_EVENT_DURATION_UNIT, EcoTerm.eventDurationUnit)
        // TODO
//        .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_NAME, EcoTerm.targett)
//        .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY, HUMBOLDT_TARGET_TAXONOMIC_SCOPE_USAGE_KEY)
//        .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_TAXONOMIC_SCOPE_TAXON_KEY, HUMBOLDT_TARGET_TAXONOMIC_SCOPE_TAXON_KEY)
        .put(OccurrenceSearchParameter.HUMBOLDT_TAXON_COMPLETENESS_PROTOCOLS, EcoTerm.taxonCompletenessProtocols)
        .put(OccurrenceSearchParameter.HUMBOLDT_IS_TAXONOMIC_SCOPE_FULLY_REPORTED, EcoTerm.isTaxonomicScopeFullyReported)
        .put(OccurrenceSearchParameter.HUMBOLDT_IS_ABSENCE_REPORTED, EcoTerm.isAbsenceReported)
        .put(OccurrenceSearchParameter.HUMBOLDT_HAS_NON_TARGET_TAXA, EcoTerm.hasNonTargetTaxa)
        .put(OccurrenceSearchParameter.HUMBOLDT_ARE_NON_TARGET_TAXA_FULLY_REPORTED, EcoTerm.areNonTargetTaxaFullyReported)
        .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_LIFE_STAGE_SCOPE, EcoTerm.targetLifeStageScope)
        .put(OccurrenceSearchParameter.HUMBOLDT_IS_LIFE_STAGE_SCOPE_FULLY_REPORTED, EcoTerm.isLifeStageScopeFullyReported)
        .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_DEGREE_OF_ESTABLISHMENT_SCOPE, EcoTerm.targetDegreeOfEstablishmentScope)
        .put(OccurrenceSearchParameter.HUMBOLDT_IS_DEGREE_OF_ESTABLISHMENT_SCOPE_FULLY_REPORTED, EcoTerm.isDegreeOfEstablishmentScopeFullyReported)
        .put(OccurrenceSearchParameter.HUMBOLDT_TARGET_GROWTH_FORM_SCOPE, EcoTerm.targetGrowthFormScope)
        .put(OccurrenceSearchParameter.HUMBOLDT_IS_GROWTH_FORM_SCOPE_FULLY_REPORTED,EcoTerm.isGrowthFormScopeFullyReported)
        .put(OccurrenceSearchParameter.HUMBOLDT_HAS_NON_TARGET_ORGANISMS, EcoTerm.hasNonTargetOrganisms)
        .put(OccurrenceSearchParameter.HUMBOLDT_COMPILATION_TYPES, EcoTerm.compilationTypes)
        .put(OccurrenceSearchParameter.HUMBOLDT_COMPILATION_SOURCE_TYPES, EcoTerm.compilationSourceTypes)
        .put(OccurrenceSearchParameter.HUMBOLDT_INVENTORY_TYPES, EcoTerm.inventoryTypes)
        .put(OccurrenceSearchParameter.HUMBOLDT_PROTOCOL_NAMES, EcoTerm.protocolNames)
        .put(OccurrenceSearchParameter.HUMBOLDT_IS_ABUNDANCE_REPORTED, EcoTerm.isAbundanceReported)
        .put(OccurrenceSearchParameter.HUMBOLDT_IS_ABUNDANCE_CAP_REPORTED, EcoTerm.isAbundanceCapReported)
        .put(OccurrenceSearchParameter.HUMBOLDT_ABUNDANCE_CAP, EcoTerm.abundanceCap)
        .put(OccurrenceSearchParameter.HUMBOLDT_IS_VEGETATION_COVER_REPORTED, EcoTerm.isVegetationCoverReported)
        .put(OccurrenceSearchParameter.HUMBOLDT_IS_LEAST_SPECIFIC_TARGET_CATEGORY_QUANTITY_INCLUSIVE, EcoTerm.isLeastSpecificTargetCategoryQuantityInclusive)
        .put(OccurrenceSearchParameter.HUMBOLDT_HAS_VOUCHERS, EcoTerm.hasVouchers)
        .put(OccurrenceSearchParameter.HUMBOLDT_VOUCHER_INSTITUTIONS, EcoTerm.voucherInstitutions)
        .put(OccurrenceSearchParameter.HUMBOLDT_HAS_MATERIAL_SAMPLES,EcoTerm.hasMaterialSamples)
        .put(OccurrenceSearchParameter.HUMBOLDT_MATERIAL_SAMPLE_TYPES, EcoTerm.materialSampleTypes)
        .put(OccurrenceSearchParameter.HUMBOLDT_SAMPLING_PERFORMED_BY, EcoTerm.samplingPerformedBy)
        .put(OccurrenceSearchParameter.HUMBOLDT_IS_SAMPLING_EFFORT_REPORTED, EcoTerm.isSamplingEffortReported)
        .put(OccurrenceSearchParameter.HUMBOLDT_SAMPLING_EFFORT_VALUE, EcoTerm.samplingEffortValue)
        .put(OccurrenceSearchParameter.HUMBOLDT_SAMPLING_EFFORT_UNIT, EcoTerm.samplingEffortUnit)

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
