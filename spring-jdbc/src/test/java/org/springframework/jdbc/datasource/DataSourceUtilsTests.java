/*
 * Copyright 2002-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.jdbc.datasource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ShardingKey;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;

import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.*;
import static org.mockito.Mockito.mock;

/**
 * Tests for {@link DataSourceUtils}.
 *
 * @author Kevin Schoenfeld
 * @author Stephane Nicoll
 */
class DataSourceUtilsTests {

	@Test
	void testConnectionNotAcquiredExceptionIsPropagated() throws SQLException {
		DataSource dataSource = mock();
		when(dataSource.getConnection()).thenReturn(null);
		assertThatThrownBy(() -> DataSourceUtils.getConnection(dataSource))
				.isInstanceOf(CannotGetJdbcConnectionException.class)
				.hasMessageStartingWith("Failed to obtain JDBC Connection")
				.hasCauseInstanceOf(IllegalStateException.class);
	}

	@Test
	void testConnectionSQLExceptionIsPropagated() throws SQLException {
		DataSource dataSource = mock();
		when(dataSource.getConnection()).thenThrow(new SQLException("my dummy exception"));
		assertThatThrownBy(() -> DataSourceUtils.getConnection(dataSource))
				.isInstanceOf(CannotGetJdbcConnectionException.class)
				.hasMessageStartingWith("Failed to obtain JDBC Connection")
				.cause().isInstanceOf(SQLException.class)
				.hasMessage("my dummy exception");
	}

	@Test
	void testCrossShardTransactionWithShardingKeyDataSourceAdapterThrowsException() throws SQLException {
		Connection connection = mock();
		ShardingKey shardingKey = mock();
		ShardingKey differentShardingKey = mock();

		ShardingKeyDataSourceAdapter shardingKeyDataSource = mock();

		// call real methods
		when(shardingKeyDataSource.unwrap(any())).thenCallRealMethod();
		when(shardingKeyDataSource.isWrapperFor(any())).thenCallRealMethod();

		// mocks
		when(shardingKeyDataSource.getShardingKeyForCurrentThread()).thenReturn(shardingKey).thenReturn(differentShardingKey);
		given(shardingKeyDataSource.getSuperShardingKeyForCurrentThread()).willReturn(null);
		given(shardingKeyDataSource.getConnection()).willReturn(connection);
		doThrow(SQLException.class).when(connection).setShardingKey(differentShardingKey);

		JdbcTransactionManager txManager = new JdbcTransactionManager(shardingKeyDataSource);
		TransactionTemplate txTemplate = new TransactionTemplate(txManager);

		txTemplate.execute(status -> {
			assertThatThrownBy(() -> DataSourceUtils.getConnection(shardingKeyDataSource)).isInstanceOf(RuntimeException.class);
			return null;
		});
	}
}

