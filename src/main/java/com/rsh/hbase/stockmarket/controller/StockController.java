package com.rsh.hbase.stockmarket.controller;

import static com.rsh.hbase.stockmarket.model.Stock.parse;
import static com.rsh.hbase.stockmarket.model.Stock.toGet;

import com.rsh.hbase.stockmarket.helper.HBaseHelper;
import com.rsh.hbase.stockmarket.model.GenericResponse;
import com.rsh.hbase.stockmarket.model.Stock;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StockController {
  private static final Logger log = LoggerFactory.getLogger(StockController.class.getName());
  private static final HBaseHelper helper = HBaseHelper.getInstance();

  @GetMapping("/stocks")
  public List<Stock> getStocks(@RequestParam(required = false) String symbol) {
    log.info(String.format("symbol: %s", symbol));
    List<Stock> records = new ArrayList<>();
    Table table = helper.getTable("market:stock");
    try {
      if (symbol != null) {
        Scan scan = new Scan();
        FilterList filters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filters.addFilter(new PrefixFilter(Bytes.toBytes(symbol)));
        scan.setFilter(filters);
        ResultScanner scanner = table.getScanner(scan);
        Stream<Result> rows = StreamSupport.stream(scanner.spliterator(), false);
        records = rows.map(Stock::parse).collect(Collectors.toList());
        scanner.close();
      } else {
        // TODO: Implement pagination
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result result : scanner) {
          records.add(parse(result));
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    return records;
  }

  @GetMapping("/stocks/{key}")
  public Stock getStockByKey(@PathVariable String key) {
    log.info(String.format("symbol: %s", key));
    Table table = helper.getTable("market:stock");
    try {
      Result result = table.get(toGet(key));
      return parse(result);
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @PostMapping("/stocks")
  public GenericResponse createStock(@RequestBody Stock stock) {
    GenericResponse response = new GenericResponse();
    System.out.println(stock.toJson());
    Table table = helper.getTable("market:stock");
    try {
      table.put(stock.toPut());
    } catch (IOException ex) {
      log.error(ex.getMessage(), ex);
      response.setMessage(ex.getMessage());
      response.setStatus("Failed");
    }
    return response;
  }

  @PatchMapping("/stocks/{key}")
  public GenericResponse patchStock(@PathVariable String key, @RequestBody Stock stock) {
    GenericResponse response = new GenericResponse();
    System.out.println(stock.toJson());
    Table table = helper.getTable("market:stock");
    try {
      table.put(stock.toPut());
    } catch (IOException ex) {
      log.error(ex.getMessage(), ex);
      response.setMessage(ex.getMessage());
      response.setStatus("Failed");
    }
    return response;
  }

  @DeleteMapping("/stocks/{key}")
  public GenericResponse deleteStock(@PathVariable String key, @RequestBody Stock stock) {
    GenericResponse response = new GenericResponse();
    System.out.println(stock.toJson());
    Table table = helper.getTable("market:stock");
    try {
      table.delete(stock.toDelete());
    } catch (IOException ex) {
      log.error(ex.getMessage(), ex);
      response.setMessage(ex.getMessage());
      response.setStatus("Failed");
    }
    return response;
  }
}
