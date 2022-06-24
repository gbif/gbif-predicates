package org.gbif.api.query;

public class QueryBuildingException extends Exception {
  public QueryBuildingException() {}

  public QueryBuildingException(String message) {
    super(message);
  }

  public QueryBuildingException(String message, Throwable cause) {
    super(message, cause);
  }

  public QueryBuildingException(Throwable cause) {
    super(cause);
  }

  public QueryBuildingException(
      String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
