package io.vertx.starter;

import com.google.common.collect.Lists;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class MainVerticle extends AbstractVerticle {

  private static Logger logger = LoggerFactory.getLogger(MainVerticle.class);

  private SQLClient mySQLClient;

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    Future<Void> steps = prepareDatabase().compose(v -> {
      logger.info("mysql connected.");
      return startHttpServer();
    });

    steps.setHandler(startFuture.completer());
//    同如下写法作用相对：
//    steps.setHandler(ar -> {
//      if (ar.succeeded()) {
//        startFuture.succeeded();
//      } else {
//        startFuture.fail(ar.cause());
//      }
//    });

  }

  private Future<Void> prepareDatabase() {

    Future<Void> future = Future.future();

    JsonObject mySQLClientConfig = new JsonObject()
      .put("host", "172.30.10.198")
      .put("username", "admin")
      .put("password", "12354")
      .put("database", "vertx_wiki");
    mySQLClient = MySQLClient.createShared(vertx, mySQLClientConfig);
    future.complete();

    return future;

  }

  private Future<Void> startHttpServer() {

    //to returned future
    Future<Void> future = Future.future();

    HttpServer httpServer = vertx.createHttpServer();
    Router router = Router.router(vertx);

    router.get("/pages").handler(context -> {

      logger.info("receive request /pages on Thread {}", Thread.currentThread().getName());

      mySQLClient.getConnection(ar -> {
        if (ar.failed()) {
          logger.error("mysql connect failed.");
          future.fail(ar.cause());
        } else {

          logger.info("success get mysql connection on Thread {}", Thread.currentThread().getName());

          SQLConnection connection = ar.result();
          connection.query("select * from pages", rs -> {

            logger.info("success connection query on Thread {}", Thread.currentThread().getName());
            connection.close();

            if (rs.failed()) {
              logger.error("mysql query failed.");
              context.fail(rs.cause());
            } else {
              logger.info("mysql query get {} results.", rs.result().getNumRows());
              context.response().putHeader("Content-Type", "application/json");
              context.response().end(rs.result().getRows().toString());
            }

          });
        }
      });
    });

    router.get("/").handler(context -> {
      context.response().end("hello vert.x!");
    });

    httpServer
      .requestHandler(router)
      .listen(8080, ar -> {
        if (ar.failed()) {
          future.fail(ar.cause());
        } else {
          future.complete();
        }
      });

    return future;

  }
}
