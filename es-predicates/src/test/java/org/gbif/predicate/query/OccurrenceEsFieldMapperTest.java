package org.gbif.predicate.query;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.api.model.predicate.GreaterThanOrEqualsPredicate;
import org.gbif.api.model.predicate.GreaterThanPredicate;
import org.gbif.api.model.predicate.SimplePredicate;

public class OccurrenceEsFieldMapperTest implements EsFieldMapper<OccurrenceSearchParameter> {

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
  public boolean isVocabulary(OccurrenceSearchParameter searchParameter) {
    return VOCABULARY_SET.contains(searchParameter);
  }

  @Override
  public boolean includeNullInPredicate(SimplePredicate<OccurrenceSearchParameter> predicate) {
    return predicate.getKey() == OccurrenceSearchParameter.DISTANCE_FROM_CENTROID_IN_METERS
        && (predicate instanceof GreaterThanOrEqualsPredicate
            || predicate instanceof GreaterThanPredicate);
  }
}
