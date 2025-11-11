package org.gbif.predicate.query;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.predicate.SimplePredicate;

public class EventEsFieldMapperTest implements EsFieldMapper<EventSearchParameter> {

  private String defaultChecklistKey = "defaultChecklistKey";

  private static final Set<EventSearchParameter> TAXONOMIC_SET =
      new HashSet<>(
          Arrays.asList(
              EventSearchParameter.SCIENTIFIC_NAME,
              EventSearchParameter.VERBATIM_SCIENTIFIC_NAME,
              EventSearchParameter.ACCEPTED_TAXON_KEY,
              EventSearchParameter.ACCEPTED_TAXON_KEY,
              EventSearchParameter.TAXON_KEY,
              EventSearchParameter.KINGDOM_KEY,
              EventSearchParameter.PHYLUM_KEY,
              EventSearchParameter.CLASS_KEY,
              EventSearchParameter.ORDER_KEY,
              EventSearchParameter.FAMILY_KEY,
              EventSearchParameter.GENUS_KEY,
              EventSearchParameter.SUBGENUS_KEY,
              EventSearchParameter.SPECIES_KEY,
              EventSearchParameter.IUCN_RED_LIST_CATEGORY,
              EventSearchParameter.TAXONOMIC_STATUS,
              EventSearchParameter.HUMBOLDT_TAXONOMIC_ISSUE));

  private static final Set<EventSearchParameter> VOCABULARY_SET =
      new HashSet<>(
          Arrays.asList(
              EventSearchParameter.HUMBOLDT_TARGET_LIFE_STAGE_SCOPE,
              EventSearchParameter.HUMBOLDT_TARGET_DEGREE_OF_ESTABLISHMENT_SCOPE));

  @Override
  public String getVerbatimFieldName(EventSearchParameter searchParameter) {
    if (searchParameter.type() == String.class) {
      return searchParameter.name().toLowerCase() + ".verbatim";
    } else {
      return searchParameter.name().toLowerCase();
    }
  }

  @Override
  public String getExactMatchFieldName(EventSearchParameter searchParameter) {
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
  public String getSearchFieldName(EventSearchParameter searchParameter) {
    return searchParameter.name();
  }

  @Override
  public boolean isVocabulary(EventSearchParameter searchParameter) {
    return VOCABULARY_SET.contains(searchParameter);
  }

  @Override
  public boolean isTaxonomic(EventSearchParameter searchParameter) {
    return TAXONOMIC_SET.contains(searchParameter);
  }

  @Override
  public String getChecklistField(String checklistKey, EventSearchParameter searchParameter) {

    if (searchParameter == EventSearchParameter.SCIENTIFIC_NAME) {
      return "classifications."
          + (checklistKey != null ? checklistKey : defaultChecklistKey)
          + ".usage.name";
    }

    return "classifications."
        + (checklistKey != null ? checklistKey : defaultChecklistKey)
        + ".taxonKeys";
  }

  @Override
  public String getNestedPath(EventSearchParameter param) {
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
  public String getDefaultChecklistKey() {
    return defaultChecklistKey;
  }

  @Override
  public List<FieldSortBuilder> getDefaultSort() {
    return List.of();
  }

  @Override
  public boolean includeNullInPredicate(SimplePredicate<EventSearchParameter> predicate) {
    return false;
  }

  @Override
  public EsField getEsField(EventSearchParameter parameter) {
    return null;
  }

  @Override
  public EsField getEsFacetField(EventSearchParameter parameter) {
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
