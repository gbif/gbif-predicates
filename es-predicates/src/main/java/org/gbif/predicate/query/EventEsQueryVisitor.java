package org.gbif.predicate.query;

import java.util.Optional;
import org.gbif.api.model.event.search.EventSearchParameter;

public class EventEsQueryVisitor extends EsQueryVisitor<EventSearchParameter> {
  public EventEsQueryVisitor(
      EsFieldMapper<EventSearchParameter> esFieldMapper, String defaultChecklistKey) {
    super(esFieldMapper, defaultChecklistKey);
  }

  @Override
  protected Optional<EventSearchParameter> getParam(String name) {
    try {
      return EventSearchParameter.lookupEventParam(name);
    } catch (Exception ex) {
      return Optional.empty();
    }
  }
}
