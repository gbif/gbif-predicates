package org.gbif.predicate.query.occurrence;

import static org.gbif.occurrence.search.es.EsFieldMapper.SEARCH_TO_ES_MAPPING;

import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.predicate.query.EsFieldMapper;

public class OccurrenceEsFieldMapper implements EsFieldMapper<OccurrenceSearchParameter> {

  @Override
  public String getVerbatimFieldName(OccurrenceSearchParameter searchParameter) {
    return SEARCH_TO_ES_MAPPING.get(searchParameter).getVerbatimFieldName();
  }

  @Override
  public String getExactMatchFieldName(OccurrenceSearchParameter searchParameter) {
    return SEARCH_TO_ES_MAPPING.get(searchParameter).getExactMatchFieldName();
  }

  @Override
  public String getGeoDistanceField() {
    return OccurrenceEsField.COORDINATE_POINT.getSearchFieldName();
  }

  @Override
  public String getGeoShapeField() {
    return OccurrenceEsField.COORDINATE_SHAPE.getSearchFieldName();
  }
}
