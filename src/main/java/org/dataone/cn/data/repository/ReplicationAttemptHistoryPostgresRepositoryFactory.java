/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright 2012. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package org.dataone.cn.data.repository;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.dataone.cn.model.repository.PostgresRepositoryConfiguration;
import org.dataone.configuration.Settings;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EnableJpaRepositories("org.dataone.cn.data.repository")
@ComponentScan("org.dataone.cn.data.repository")
public class ReplicationAttemptHistoryPostgresRepositoryFactory extends
        PostgresRepositoryConfiguration {

    private static final String urlProp = "datasource.postgres.replication.history.url";
    private static final String driverClassProp = "datasource.postgres.replication.history.driverClass";
    private static final String usernameProp = "datasource.postgres.replication.history.user";
    private static final String passwordProperty = "datasource.postgres.replication.history.password";
    private static final String initialPoolSizeProperty = "datasource.postgres.replication.history.initialSize";
    private static final String maxPoolSizeProperty = "datasource.postgres.replication.history.maxSize";

    private static final String url = Settings.getConfiguration().getString(urlProp);
    private static final String driverClass = Settings.getConfiguration()
            .getString(driverClassProp);
    private static final String username = Settings.getConfiguration().getString(usernameProp);
    private static final String password = Settings.getConfiguration().getString(passwordProperty);
    private static final String initialPoolSize = Settings.getConfiguration().getString(
            initialPoolSizeProperty);
    private static final String maxPoolSize = Settings.getConfiguration().getString(
            maxPoolSizeProperty);

    private static BasicDataSource postgresDataSource;

    public ReplicationAttemptHistoryRepository getReplicationTryHistoryRepository() {
        return context.getBean(ReplicationAttemptHistoryRepository.class);
    }

    @Bean
    public DataSource dataSource() {
        if (postgresDataSource == null) {
            initDataSource();
        }
        return postgresDataSource;
    }

    private void initDataSource() {
        postgresDataSource = new BasicDataSource();
        postgresDataSource.setUrl(url);
        postgresDataSource.setDriverClassName(driverClass);
        postgresDataSource.setUsername(username);
        postgresDataSource.setPassword(password);
        postgresDataSource.setInitialSize(Integer.valueOf(initialPoolSize).intValue());
        postgresDataSource.setMaxActive(Integer.valueOf(maxPoolSize).intValue());
    }

    public String getPackagesToScan() {
        return "org.dataone.cn.data.repository";
    }
}
