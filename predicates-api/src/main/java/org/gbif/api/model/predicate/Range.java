package org.gbif.api.model.predicate;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/** Simplified version of guava's {@code Range}. */
public class Range<T extends Comparable<? super T>> {

  /** Lower bound. Unbound if it is {@code null}. */
  private T from;

  /** Upper bound. Unbound if it is {@code null}. */
  private T to;

  /**
   * Create a range with bounds {@code from} and {@code to}. Use factory method instead.
   *
   * @throws IllegalArgumentException if {@code from} is greater than {@code to}
   */
  private Range(T from, T to) {
    if (from != null && to != null && from.compareTo(to) > 0) {
      throw new IllegalArgumentException(String.format("Invalid range: (%s,%s)", from, to));
    }
    this.from = from;
    this.to = to;
  }

  /** Factory method. */
  public static <T extends Comparable<? super T>> Range<T> closed(T from, T to) {
    return new Range<>(from, to);
  }

  /** Returns {@code true} if this range has a lower endpoint. */
  public boolean hasLowerBound() {
    return from != null;
  }

  /** Returns {@code true} if this range has an upper endpoint. */
  public boolean hasUpperBound() {
    return to != null;
  }

  /** Returns {@code true} if {@code value} is within the bounds of this range. */
  public boolean contains(@NotNull T value) {
    return from.compareTo(value) <= 0 && to.compareTo(value) >= 0;
  }

  /**
   * Returns {@code true} if the bounds of {@code other} do not extend outside the bounds of this
   * range.
   */
  public boolean encloses(@NotNull Range<T> other) {
    return from.compareTo(other.from) <= 0 && to.compareTo(other.to) >= 0;
  }

  @Nullable
  public T lowerEndpoint() {
    return from;
  }

  @Nullable
  public T upperEndpoint() {
    return to;
  }
}
