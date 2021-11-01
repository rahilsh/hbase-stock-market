package com.rsh.hbase.stockmarket.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

@Data
public class Stock {

  private static final byte[] INFO_CF = Bytes.toBytes("info");
  private static final byte[] DATE = Bytes.toBytes("date");
  private static final byte[] OPEN = Bytes.toBytes("open");
  private static final byte[] HIGH = Bytes.toBytes("high");
  private static final byte[] LOW = Bytes.toBytes("low");
  private static final byte[] CLOSE = Bytes.toBytes("close");
  private static final byte[] VOLUME = Bytes.toBytes("volume");
  private static final byte[] ADJCLOSE = Bytes.toBytes("adjclose");
  private static final byte[] SYMBOL = Bytes.toBytes("symbol");
  private String rowKey;
  private String date;
  private String open;
  private String high;
  private String low;
  private String close;
  private String volume;
  private String adjClose;
  private String symbol;

  public static Stock parse(Result result) {
    Stock record = new Stock();
    record.setRowKey(Bytes.toString(result.getRow()));
    record.setDate(Bytes.toString(result.getValue(INFO_CF, DATE)));
    record.setOpen(Bytes.toString(result.getValue(INFO_CF, OPEN)));
    record.setHigh(Bytes.toString(result.getValue(INFO_CF, HIGH)));
    record.setLow(Bytes.toString(result.getValue(INFO_CF, LOW)));
    record.setClose(Bytes.toString(result.getValue(INFO_CF, CLOSE)));
    record.setVolume(Bytes.toString(result.getValue(INFO_CF, VOLUME)));
    record.setAdjClose(Bytes.toString(result.getValue(INFO_CF, ADJCLOSE)));
    record.setSymbol(Bytes.toString(result.getValue(INFO_CF, SYMBOL)));
    return record;
  }

  public static Get toGet(String string) {
    return new Get(Bytes.toBytes(string));
  }

  public byte[] toBytes(String s) {
    if (s != null) {
      return Bytes.toBytes(s);
    }
    return null;
  }

  public Put toPut() {
    return new Put(Bytes.toBytes(this.symbol + "-" + this.date))
        .addColumn(INFO_CF, DATE, toBytes(this.date))
        .addColumn(INFO_CF, OPEN, toBytes(this.open))
        .addColumn(INFO_CF, HIGH, toBytes(this.high))
        .addColumn(INFO_CF, LOW, toBytes(this.low))
        .addColumn(INFO_CF, CLOSE, toBytes(this.close))
        .addColumn(INFO_CF, VOLUME, toBytes(this.volume))
        .addColumn(INFO_CF, ADJCLOSE, toBytes(this.adjClose))
        .addColumn(INFO_CF, SYMBOL, toBytes(this.symbol));
  }

  public Delete toDelete() {
    return new Delete(Bytes.toBytes(this.symbol + "-" + this.date));
  }

  public String toJson() {
    ObjectMapper mapper = new ObjectMapper();
    String json = null;
    try {
      json = mapper.writeValueAsString(this);
    } catch (JsonProcessingException ex) {
      ex.printStackTrace();
    }
    return json;
  }
}
