package com.rsh.hbase.stockmarket.model;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class GenericResponse {
  private String status = "Success";
  private String message = null;
}
