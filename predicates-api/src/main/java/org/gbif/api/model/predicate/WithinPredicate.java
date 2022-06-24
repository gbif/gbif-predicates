/*
 * Copyright 2020 Global Biodiversity Information Facility (GBIF)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.api.model.predicate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.text.ParseException;
import java.util.Objects;
import java.util.StringJoiner;
import javax.validation.constraints.NotNull;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.operation.valid.IsValidOp;
import org.locationtech.spatial4j.context.jts.DatelineRule;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

/**
 * This predicate checks if an occurrence location falls within the given WKT geometry {@code
 * value}.
 */
public class WithinPredicate implements Predicate {

  //  private static final Logger LOG = LoggerFactory.getLogger(WithinPredicate.class);

  @NotNull private final String geometry;

  /**
   * Builds a new within predicate for a single, simple geometry as <a
   * href="http://en.wikipedia.org/wiki/Well-known_text">Well Known Text</a> (WKT). Multi geometries
   * like MULTIPOLYGON are not supported and multiple predicates should be used instead. <br>
   * The validation implemented does a basic syntax check for the following simple geometries, but
   * does not verify that the resulting geometries are topologically valid (see the OGC SFS
   * specification).
   *
   * <ul>
   *   <li>POINT
   *   <li>LINESTRING
   *   <li>POLYGON
   *   <li>LINEARRING
   * </ul>
   *
   * <strong>Unlike other predicates, this validation only logs in case of an invalid
   * string.</strong> This is because the WKT parser has been changed over time, and some old
   * strings are not valid according to the current parser.
   *
   * @param geometry
   */
  @JsonCreator
  public WithinPredicate(@JsonProperty("geometry") String geometry) {
    Objects.requireNonNull(geometry, "<geometry> may not be null");
    try {
      // test if it is a valid WKT
      validateGeometry(geometry);
    } catch (IllegalArgumentException e) {
      // Log invalid strings, but continue - the geometry parser has changed over time, and some
      // once-valid strings
      // are no longer considered valid.  See https://github.com/gbif/gbif-api/issues/48.
      //      LOG.warn("Invalid geometry string {}: {}", geometry, e.getMessage());
    }
    this.geometry = geometry;
  }

  public String getGeometry() {
    return geometry;
  }

  private static void validateGeometry(String wellKnownText) {
    JtsSpatialContextFactory spatialContextFactory = new JtsSpatialContextFactory();
    spatialContextFactory.normWrapLongitude = true;
    spatialContextFactory.srid = 4326;
    spatialContextFactory.datelineRule = DatelineRule.ccwRect;
    WKTReader reader =
        new WKTReader(spatialContextFactory.newSpatialContext(), spatialContextFactory);

    try {
      Shape shape = reader.parse(wellKnownText);
      if (shape instanceof JtsGeometry) {
        Geometry geometry = ((JtsGeometry) shape).getGeom();
        IsValidOp validator = new IsValidOp(geometry);
        if (!validator.isValid()) {
          throw new IllegalArgumentException("Invalid geometry: " + validator.getValidationError());
        } else if (geometry.isEmpty()) {
          throw new IllegalArgumentException("Empty geometry: " + wellKnownText);
        } else if (geometry instanceof Polygon && geometry.getArea() == 0.0) {
          throw new IllegalArgumentException("Polygon with zero area: " + wellKnownText);
        } else {
          switch (geometry.getGeometryType().toUpperCase()) {
            case "POINT":
            case "LINESTRING":
            case "POLYGON":
            case "MULTIPOLYGON":
              return;
            case "MULTIPOINT":
            case "MULTILINESTRING":
            case "GEOMETRYCOLLECTION":
            default:
              throw new IllegalArgumentException(
                  "Unsupported simple WKT (unsupported type "
                      + geometry.getGeometryType()
                      + "): "
                      + wellKnownText);
          }
        }
      }
    } catch (ParseException | AssertionError var8) {
      throw new IllegalArgumentException(
          "Cannot parse simple WKT: " + wellKnownText + " " + var8.getMessage());
    } catch (InvalidShapeException var9) {
      throw new IllegalArgumentException(
          "Invalid shape in WKT: " + wellKnownText + " " + var9.getMessage());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    WithinPredicate that = (WithinPredicate) o;
    return Objects.equals(geometry, that.geometry);
  }

  @Override
  public int hashCode() {
    return Objects.hash(geometry);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", WithinPredicate.class.getSimpleName() + "[", "]")
        .add("geometry='" + geometry + "'")
        .toString();
  }
}
