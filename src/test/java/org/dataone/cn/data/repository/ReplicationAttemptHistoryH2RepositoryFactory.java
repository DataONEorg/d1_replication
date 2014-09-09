package org.dataone.cn.data.repository;

import org.dataone.cn.model.repository.H2RepositoryConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EnableJpaRepositories("org.dataone.cn.data.repository")
@ComponentScan("org.dataone.cn.data.repository")
public class ReplicationAttemptHistoryH2RepositoryFactory extends H2RepositoryConfiguration {

    public ReplicationAttemptHistoryRepository getReplicationTryHistoryRepository() {
    	this.initContext();
    	return context.getBean(ReplicationAttemptHistoryRepository.class);
    }

    @Override
    public String getPackagesToScan() {
        return "org.dataone.cn.data.repository";
    }
}
