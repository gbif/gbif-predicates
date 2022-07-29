package org.gbif.api.model.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import javax.annotation.Nullable;

public class RangeValue {

  @JsonCreator
  public RangeValue(
      @Nullable @JsonProperty("gte") String gte, @Nullable @JsonProperty("lte") String lte) {
    this.gte = gte;
    this.lte = lte;
  }

  @JsonProperty("gte")
  String gte;

  @JsonProperty("lte")
  String lte;

  public String getGte() {
    return gte;
  }

  public String getLte() {
    return lte;
  }
}
