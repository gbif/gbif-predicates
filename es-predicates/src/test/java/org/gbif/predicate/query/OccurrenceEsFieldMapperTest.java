package org.gbif.predicate.query;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.predicate.GreaterThanPredicate;
import org.gbif.api.model.predicate.SimplePredicate;

public class OccurrenceEsFieldMapperTest implements EsFieldMapper<OccurrenceSearchParameter> {

  private String defaultChecklistKey = "defaultChecklistKey";

  private static final Set<OccurrenceSearchParameter> TAXONOMIC_SET =
      new HashSet<>(
          Arrays.asList(
              OccurrenceSearchParameter.SCIENTIFIC_NAME,
              OccurrenceSearchParameter.VERBATIM_SCIENTIFIC_NAME,
              OccurrenceSearchParameter.ACCEPTED_TAXON_KEY,
              OccurrenceSearchParameter.ACCEPTED_TAXON_KEY,
              OccurrenceSearchParameter.TAXON_KEY,
              OccurrenceSearchParameter.KINGDOM_KEY,
              OccurrenceSearchParameter.PHYLUM_KEY,
              OccurrenceSearchParameter.CLASS_KEY,
              OccurrenceSearchParameter.ORDER_KEY,
              OccurrenceSearchParameter.FAMILY_KEY,
              OccurrenceSearchParameter.GENUS_KEY,
              OccurrenceSearchParameter.SUBGENUS_KEY,
              OccurrenceSearchParameter.SPECIES_KEY,
              OccurrenceSearchParameter.IUCN_RED_LIST_CATEGORY,
              OccurrenceSearchParameter.TAXONOMIC_STATUS,
              OccurrenceSearchParameter.TAXONOMIC_ISSUE));

  private static final Set<OccurrenceSearchParameter> VOCABULARY_SET =
      new HashSet<>(
          Arrays.asList(
              OccurrenceSearchParameter.LIFE_STAGE,
              OccurrenceSearchParameter.PATHWAY,
              OccurrenceSearchParameter.DEGREE_OF_ESTABLISHMENT,
              OccurrenceSearchParameter.ESTABLISHMENT_MEANS,
              OccurrenceSearchParameter.EARLIEST_EON_OR_LOWEST_EONOTHEM,
              OccurrenceSearchParameter.LATEST_EON_OR_HIGHEST_EONOTHEM,
              OccurrenceSearchParameter.EARLIEST_ERA_OR_LOWEST_ERATHEM,
              OccurrenceSearchParameter.LATEST_ERA_OR_HIGHEST_ERATHEM,
              OccurrenceSearchParameter.EARLIEST_PERIOD_OR_LOWEST_SYSTEM,
              OccurrenceSearchParameter.LATEST_PERIOD_OR_HIGHEST_SYSTEM,
              OccurrenceSearchParameter.EARLIEST_EPOCH_OR_LOWEST_SERIES,
              OccurrenceSearchParameter.LATEST_EPOCH_OR_HIGHEST_SERIES,
              OccurrenceSearchParameter.EARLIEST_AGE_OR_LOWEST_STAGE,
              OccurrenceSearchParameter.LATEST_AGE_OR_HIGHEST_STAGE));

  @Override
  public String getVerbatimFieldName(OccurrenceSearchParameter searchParameter) {
    if (searchParameter.type() == String.class) {
      return searchParameter.name().toLowerCase() + ".verbatim";
    } else {
      return searchParameter.name().toLowerCase();
    }
  }

  @Override
  public String getExactMatchFieldName(OccurrenceSearchParameter searchParameter) {
    if (searchParameter.type() == String.class) {
      return searchParameter.name().toLowerCase() + ".keyword";
    } else {
      return searchParameter.name().toLowerCase();
    }
  }

  @Override
  public String getGeoDistanceField() {
    return "coordinates";
  }

  @Override
  public String getGeoShapeField() {
    return "scoordinates";
  }

  @Override
  public String getSearchFieldName(OccurrenceSearchParameter searchParameter) {
    return searchParameter.name();
  }

  @Override
  public boolean isVocabulary(OccurrenceSearchParameter searchParameter) {
    return VOCABULARY_SET.contains(searchParameter);
  }

  @Override
  public boolean isTaxonomic(OccurrenceSearchParameter searchParameter) {
    return TAXONOMIC_SET.contains(searchParameter);
  }

  @Override
  public String getChecklistField(String checklistKey, OccurrenceSearchParameter searchParameter) {

    if (searchParameter == OccurrenceSearchParameter.SCIENTIFIC_NAME) {
      return "classifications."
          + (checklistKey != null ? checklistKey : defaultChecklistKey)
          + ".usage.name";
    }

    return "classifications."
        + (checklistKey != null ? checklistKey : defaultChecklistKey)
        + ".taxonKeys";
  }

  @Override
  public String getNestedPath(OccurrenceSearchParameter param) {
    if (param.name().startsWith("HUMBOLDT_")) {
      return "event.humboldt";
    }
    return null;
  }

  @Override
  public String getFullTextField() {
    return "all";
  }

  @Override
  public List<FieldSortBuilder> getDefaultSort() {
    return List.of();
  }

  @Override
  public boolean includeNullInPredicate(SimplePredicate<OccurrenceSearchParameter> predicate) {
    return predicate.getKey() == OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS
        && (predicate instanceof GreaterThanOrEqualsPredicate
            || predicate instanceof GreaterThanPredicate);
  }

  @Override
  public EsField getEsField(OccurrenceSearchParameter parameter) {
    return null;
  }

  @Override
  public EsField getEsFacetField(OccurrenceSearchParameter parameter) {
    return null;
  }

  @Override
  public boolean isDateField(EsField esField) {
    return false;
  }

  @Override
  public EsField getGeoDistanceEsField() {
    return null;
  }

  @Override
  public EsField getGeoShapeEsField() {
    return null;
  }
}
