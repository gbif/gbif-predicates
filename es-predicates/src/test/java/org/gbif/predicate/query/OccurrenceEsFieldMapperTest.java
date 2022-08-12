package org.gbif.predicate.query;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;

public class OccurrenceEsFieldMapperTest implements EsFieldMapper<OccurrenceSearchParameter> {

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
}
