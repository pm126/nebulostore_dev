package org.nebulostore.persistence;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.DataSource;

import com.google.common.base.Function;

import org.apache.log4j.Logger;
import org.h2.jdbcx.JdbcConnectionPool;

public class SQLKeyValueStore<T> implements KeyValueStore<T> {

  private static final String TABLE_NAME = "keyvaluestore";
  private final Function<T, byte[]> serializer_;
  private final Function<byte[], T> deserializer_;
  private Connection connection_;
  private final boolean canUpdateKeys_;

  public SQLKeyValueStore(String host, String port, String database, String user, String password,
      boolean canUpdateKeys, Function<T, byte[]> serializer, Function<byte[], T> deserializer)
      throws IOException {
    serializer_ = serializer;
    deserializer_ = deserializer;
    canUpdateKeys_ = canUpdateKeys;
    try {
      connection_ = DriverManager
          .getConnection("jdbc:postgresql://" + host + ":" + port + "/" + database,
              user, password);
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Use an in-memory H2 database.
   */
  public SQLKeyValueStore(Function<T, byte[]> serializer, Function<byte[], T> deserializer)
      throws IOException {
    canUpdateKeys_ = true;
    serializer_ = serializer;
    deserializer_ = deserializer;
    try {
      DataSource ds = JdbcConnectionPool.create("jdbc:h2:mem:test", "user", "password");
      connection_ = ds.getConnection();
      connection_.createStatement().execute("SET MODE PostgreSQL");
      connection_.createStatement().execute("DROP ALL OBJECTS");
      connection_.createStatement().execute(
          "CREATE TABLE keyvaluestore (key_str VARCHAR(255) PRIMARY KEY, value BINARY )");
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public void put(String key, T value) throws IOException {
    T oldVal = get(key);
    if (oldVal != null && canUpdateKeys_) {
      update(key, value);
    } else if (oldVal != null) {
      throw new IOException("Object with key " + key + " already exists in the database!");
    } else {
      String sql = "INSERT INTO " + TABLE_NAME + " (key_str, value) VALUES (?, ?)";
      try {
        PreparedStatement preparedStatement = connection_.prepareStatement(sql);
        try {
          preparedStatement.setString(1, key);
          preparedStatement.setBytes(2, serializer_.apply(value));
          preparedStatement.execute();
        } finally {
          preparedStatement.close();
        }
      } catch (SQLException e) {
        throw new IOException(e.getMessage());
      }
    }
  }

  public T executeSelectKey(String key) throws IOException {
    String sql = "SELECT value FROM " + TABLE_NAME + " WHERE key_str = ? ";
    try {
      PreparedStatement preparedStatement = connection_.prepareStatement(sql);
      try {
        preparedStatement.setString(1, key);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
          return deserializer_.apply(resultSet.getBytes(1));
        }
      } finally {
        preparedStatement.close();
      }
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
    throw new IOException("Element " + key + " does not exits!");
  }

  @Override
  public T get(String key) {
    try {
      return executeSelectKey(key);
    } catch (IOException e) {
      Logger.getLogger(SQLKeyValueStore.class).warn("Error while getting a value from database: ",
          e);
      return null;
    }
  }

  @Override
  public void delete(String key) throws IOException {
    String sql = "DELETE FROM " + TABLE_NAME + " WHERE key_str = ?";
    try {
      PreparedStatement preparedStatement = connection_.prepareStatement(sql);
      try {
        preparedStatement.setString(1, key);
        preparedStatement.execute();
      } finally {
        preparedStatement.close();
      }
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  private void startTransaction() throws IOException {
    try {
      PreparedStatement preparedStatement = connection_.prepareStatement("BEGIN");
      preparedStatement.execute();
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  private void commitTransaction() throws IOException {
    try {
      PreparedStatement preparedStatement = connection_.prepareStatement("COMMIT");
      preparedStatement.execute();
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  private void rollbackTransaction() throws IOException {
    try {
      PreparedStatement preparedStatement = connection_.prepareStatement("ROLLBACK");
      preparedStatement.execute();
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  private void update(String key, T value) throws IOException {
    String sql = "UPDATE " + TABLE_NAME + " SET value = ? WHERE key_str = ?";
    try {
      PreparedStatement preparedStatement = connection_.prepareStatement(sql);
      try {
        preparedStatement.setBytes(1, serializer_.apply(value));
        preparedStatement.setString(2, key);
        preparedStatement.execute();
      } finally {
        preparedStatement.close();
      }
    } catch (SQLException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public void performTransaction(String key, Function<T, T> function)
      throws IOException {
    startTransaction();
    try {
      T oldVal;
      try {
        oldVal = executeSelectKey(key);
      } catch (IOException e) {
        oldVal = null;
      }
      T newVal = function.apply(oldVal);
      put(key, newVal);
      commitTransaction();
    } catch (IOException e) {
      rollbackTransaction();
      throw e;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    if (connection_ != null) {
      connection_.close();
    }
    super.finalize();
  }

}
