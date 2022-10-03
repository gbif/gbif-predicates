package org.gbif.api.model.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import javax.annotation.Nullable;

public class RangeValue {

  @JsonCreator
  public RangeValue(
      @Nullable @JsonProperty("gte") String gte,
      @Nullable @JsonProperty("gt") String gt,
      @Nullable @JsonProperty("lte") String lte,
      @Nullable @JsonProperty("gt") String lt) {

    if (Objects.isNull(gte) && Objects.isNull(gt)) {
      throw new NullPointerException("Specify gte or gt, not both");
    }
    if (Objects.isNull(lte) && Objects.isNull(lt)) {
      throw new NullPointerException("Specify lte or lt, not both");
    }

    if (!Objects.isNull(gte) && !Objects.isNull(gt)) {
      throw new IllegalArgumentException("Specify gte or gt, not both");
    }
    if (lte != null && lt != null) {
      throw new IllegalArgumentException("Specify lte or lt, not both");
    }
    this.gte = gte;
    this.lte = lte;
    this.gt = gt;
    this.lt = lt;
  }

  @JsonProperty("gt")
  String gt;

  @JsonProperty("lt")
  String lt;

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

  public String getGt() {
    return gt;
  }

  public String getLt() {
    return lt;
  }
}
