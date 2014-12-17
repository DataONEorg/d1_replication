package org.dataone.cn.data.repository;

import org.dataone.cn.model.repository.H2RepositoryConfiguration;
import org.dataone.service.cn.replication.ReplicationRepositoryFactory;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EnableJpaRepositories("org.dataone.cn.data.repository")
@ComponentScan("org.dataone.cn.data.repository")
public class ReplicationH2RepositoryFactory extends H2RepositoryConfiguration implements
        ReplicationRepositoryFactory {

    public ReplicationAttemptHistoryRepository getReplicationTryHistoryRepository() {
        initContext();
        return context.getBean(ReplicationAttemptHistoryRepository.class);
    }

    public ReplicationTaskRepository getReplicationTaskRepository() {
        initContext();
        return context.getBean(ReplicationTaskRepository.class);
    }

    @Override
    public String getPackagesToScan() {
        return "org.dataone.cn.data.repository";
    }
}
